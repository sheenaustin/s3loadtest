"""Utility functions — retry logic, data generation, formatting."""

from __future__ import annotations

import hashlib
import logging
import multiprocessing
import os
import random
import string
import struct
import time
from typing import Any

from botocore.exceptions import ClientError

from s3loadtest.config import (
    CPU_ALLOCATION_PERCENTAGE,
    DATA_DIR,
    IO_THREAD_MULTIPLIER,
    RETRY_BASE_DELAY,
    RETRY_MAX_ATTEMPTS,
    RETRY_MAX_DELAY,
    SIZE_RANGES,
    TEST_TARGET_LOAD,
)


def is_not_found_error(exc: Exception) -> bool:
    """Check if exception is a 404/NoSuchKey error.

    Args:
        exc: Exception to check.

    Returns:
        True if the error indicates the object was not found.
    """
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        return code in ("NoSuchKey", "404", "NotFound")
    return "NoSuchKey" in str(exc) or "404" in str(exc)


def retry_with_backoff(
    func: Any,
    *,
    max_retries: int | None = None,
    base_delay: float | None = None,
    max_delay: float | None = None,
    logger: logging.Logger | None = None,
) -> Any:
    """Retry S3 operations with exponential backoff.

    Handles rate limiting and transient errors. Connection errors
    (refused, reset) are NOT retried so s3pool's circuit breaker
    can quickly remove bad endpoints from rotation.

    Args:
        func: Function to execute.
        max_retries: Maximum retry attempts.
        base_delay: Initial delay in seconds.
        max_delay: Maximum delay between retries.
        logger: Optional logger for retry events.

    Returns:
        Result from func() if successful.

    Raises:
        Exception: If all retries exhausted or non-retryable error.
    """
    if max_retries is None:
        max_retries = RETRY_MAX_ATTEMPTS
    if base_delay is None:
        base_delay = RETRY_BASE_DELAY
    if max_delay is None:
        max_delay = RETRY_MAX_DELAY

    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func()
        except ClientError as exc:
            error_code = exc.response.get(
                "Error", {},
            ).get("Code", "")
            status_code = exc.response.get(
                "ResponseMetadata", {},
            ).get("HTTPStatusCode", 0)

            retryable_codes = (
                "RequestTimeout",
                "RequestTimeoutException",
                "PriorRequestNotComplete",
                "SlowDown",
                "ServiceUnavailable",
                "InternalError",
            )
            retryable_statuses = (429, 503, 500, 502, 504)

            if (
                error_code in retryable_codes
                or status_code in retryable_statuses
            ):
                last_exception = exc
            else:
                raise

            if attempt < max_retries:
                delay = min(
                    base_delay * (2 ** attempt), max_delay,
                )
                jitter = random.uniform(0, delay * 0.3)
                if logger:
                    logger.debug(
                        f"Retry {attempt + 1}/{max_retries}: "
                        f"{error_code or status_code}, "
                        f"backoff {delay + jitter:.2f}s"
                    )
                time.sleep(delay + jitter)

        except Exception as exc:
            error_str = str(exc).lower()
            connection_errors = (
                "connection refused",
                "connection reset",
                "errno 111",
            )
            if any(e in error_str for e in connection_errors):
                if logger:
                    logger.warning(
                        f"Connection error (no retry): {exc}"
                    )
                raise

            transient_markers = (
                "timeout", "429", "503", "500",
                "502", "504", "slow down",
            )
            if any(m in error_str for m in transient_markers):
                last_exception = exc
                if attempt < max_retries:
                    delay = min(
                        base_delay * (2 ** attempt), max_delay,
                    )
                    jitter = random.uniform(0, delay * 0.3)
                    if logger:
                        logger.debug(
                            f"Retry {attempt + 1}/{max_retries}"
                            f": {type(exc).__name__}, "
                            f"backoff {delay + jitter:.2f}s"
                        )
                    time.sleep(delay + jitter)
            else:
                raise

    if last_exception:
        if logger:
            logger.warning(
                f"All {max_retries} retries exhausted: "
                f"{last_exception}"
            )
        raise last_exception


def calculate_threads_for_test(test_name: str) -> int:
    """Calculate thread count for a test based on CPU count.

    S3 operations are I/O-bound (99%+ time spent waiting on network).
    We use IO_THREAD_MULTIPLIER (default 8x) to saturate the network
    rather than the CPU. On a 64-core machine this gives 512 threads.

    Args:
        test_name: Name of the test.

    Returns:
        Number of threads to use.
    """
    cpu_count = multiprocessing.cpu_count()
    target_load = TEST_TARGET_LOAD.get(test_name, 1.0)
    threads = int(
        cpu_count
        * CPU_ALLOCATION_PERCENTAGE
        * IO_THREAD_MULTIPLIER
        * target_load
    )
    return max(1, threads)


def random_object_size(
    size_list: list[str],
) -> tuple[int, str]:
    """Get random size from list.

    Args:
        size_list: List of size names to choose from.

    Returns:
        Tuple of (size_in_bytes, size_name).
    """
    size_name = random.choice(size_list)
    min_size, max_size = SIZE_RANGES[size_name]
    return random.randint(min_size, max_size), size_name


def generate_random_suffix(length: int = 32) -> str:
    """Generate random suffix with S3-safe characters.

    Args:
        length: Length of suffix to generate.

    Returns:
        Random alphanumeric string safe for S3 keys.
    """
    safe_chars = (
        string.ascii_lowercase
        + string.ascii_uppercase
        + string.digits
        + "-_."
    )
    return "".join(
        random.choice(safe_chars) for _ in range(length)
    )


def generate_data(size: int) -> bytes:
    """Generate random data of specified size.

    Args:
        size: Number of bytes to generate.

    Returns:
        Random bytes.
    """
    return os.urandom(size)


def get_deterministic_filename(
    size_bytes: int,
    size_name: str,
) -> str:
    """Generate deterministic filename for pre-generated data.

    Args:
        size_bytes: File size in bytes.
        size_name: Human-readable size name.

    Returns:
        Deterministic filename string.
    """
    hash_input = f"loadtest-data-{size_bytes}".encode("utf-8")
    hash_digest = hashlib.md5(hash_input).hexdigest()[:8]
    return f"data-{size_name}-{hash_digest}.bin"


def read_test_data(size: int) -> bytes:
    """Read data from pre-generated file with unique footer.

    The footer breaks file-level deduplication by including
    timestamp, PID, and random data.

    Args:
        size: Desired data size in bytes.

    Returns:
        Bytes of the requested size.
    """
    size_tiers = [
        (1024, "1kb"),
        (10 * 1024, "10kb"),
        (100 * 1024, "100kb"),
        (1024 * 1024, "1mb"),
        (10 * 1024 * 1024, "10mb"),
        (100 * 1024 * 1024, "100mb"),
    ]

    base_size = size_tiers[-1][0]
    size_name = size_tiers[-1][1]
    for tier_size, tier_name in size_tiers:
        if size <= tier_size:
            base_size = tier_size
            size_name = tier_name
            break

    filepath = os.path.join(
        DATA_DIR,
        get_deterministic_filename(base_size, size_name),
    )
    read_size = min(size - 32, base_size)

    try:
        with open(filepath, "rb") as f:
            data = f.read(read_size)
    except OSError as exc:
        print(
            f"Warning: Error reading {filepath}: {exc}, "
            f"using random data"
        )
        return os.urandom(size)

    footer = struct.pack(
        "QQQQ",
        int(time.time() * 1_000_000_000),
        os.getpid(),
        id(data),
        random.randint(0, 2**64 - 1),
    )

    total_size = len(data) + len(footer)
    if total_size < size:
        padding = os.urandom(size - total_size)
        return data + footer + padding

    return data + footer


def parse_duration(duration_str: str) -> int:
    """Parse duration string to seconds.

    Args:
        duration_str: Duration like '5m', '1h', '2d', '1w'.

    Returns:
        Duration in seconds.

    Raises:
        ValueError: If format is invalid.
    """
    duration_str = duration_str.strip().lower()

    if not duration_str:
        raise ValueError("Duration cannot be empty")

    if duration_str[-1] in ("m", "h", "d", "w"):
        try:
            value = int(duration_str[:-1])
            unit = duration_str[-1]
        except ValueError:
            raise ValueError(
                f"Invalid duration format: {duration_str}"
            )
    else:
        raise ValueError(
            f"Invalid duration format: {duration_str}. "
            f"Must end with m/h/d/w"
        )

    if value <= 0:
        raise ValueError(
            f"Duration must be positive: {duration_str}"
        )

    multipliers = {
        "m": 60, "h": 3600, "d": 86400, "w": 604800,
    }
    return value * multipliers[unit]


def format_duration(seconds: int) -> str:
    """Format seconds into human-readable duration.

    Args:
        seconds: Duration in seconds.

    Returns:
        Human-readable duration string.
    """
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    elif seconds < 86400:
        return f"{seconds // 3600}h"
    elif seconds < 604800:
        return f"{seconds // 86400}d"
    else:
        return f"{seconds // 604800}w"


def format_bytes(size: float) -> str:
    """Format bytes as human-readable string.

    Args:
        size: Size in bytes.

    Returns:
        Human-readable size string.
    """
    if size < 1024:
        return f"{size}B"
    elif size < 1024**2:
        return f"{size / 1024:.1f}KB"
    elif size < 1024**3:
        return f"{size / 1024**2:.1f}MB"
    else:
        return f"{size / 1024**3:.1f}GB"
