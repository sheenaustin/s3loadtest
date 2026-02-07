"""S3 Client Factory — Creates S3 clients with configuration pre-loaded.

Usage::

    from s3loadtest.s3_client import S3Client

    client = S3Client()                        # Default: boto3
    client = S3Client(delay=0.1)               # With simulated latency
    client = S3Client(backend="proxy")         # Use s3pool proxy
    client = S3Client(backend="minio")         # Use minio-py
"""

from __future__ import annotations

from typing import Any

from s3loadtest.config import (
    S3_BACKEND,
    S3_BUCKET,
    S3_ENDPOINTS,
    S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY,
    S3_REGION,
)

_BACKEND_CACHE: dict[str, type] = {}


def _get_backend_class(backend_name: str | None = None) -> type:
    """Resolve backend name to class (cached).

    Args:
        backend_name: Backend identifier. If None, uses
            ``S3_BACKEND`` from config.

    Returns:
        The S3 client class for the requested backend.

    Raises:
        ValueError: If the backend name is not recognized.
    """
    name = (backend_name or S3_BACKEND).lower()
    if name in _BACKEND_CACHE:
        return _BACKEND_CACHE[name]

    from s3loadtest.backends import (
        S3ClientBoto3,
        S3ClientMinio,
        S3ClientRustProxy,
    )

    mapping: dict[str, type] = {
        "boto3": S3ClientBoto3,
        "proxy": S3ClientRustProxy,
        "rustproxy": S3ClientRustProxy,
        "minio": S3ClientMinio,
    }

    cls = mapping.get(name)
    if cls is None:
        available = ", ".join(mapping.keys())
        raise ValueError(
            f"Unknown S3 backend '{name}'. "
            f"Available: {available}"
        )

    _BACKEND_CACHE[name] = cls
    return cls


def S3Client(
    *,
    delay: float = 0,
    bandwidth_bps: int = 0,
    backend: str | None = None,
) -> Any:
    """Create an S3 client with pre-loaded configuration.

    Args:
        delay: Artificial latency in seconds per operation.
        bandwidth_bps: Bandwidth limit in bytes/sec (0 = unlimited).
        backend: Override backend (``boto3``, ``proxy``, ``minio``).

    Returns:
        S3 client instance for the selected backend.
    """
    cls = _get_backend_class(backend)

    if cls.__name__ == "S3ClientRustProxy":
        return cls(
            bucket=S3_BUCKET,
            delay=delay,
            bandwidth_bps=bandwidth_bps,
        )
    else:
        return cls(
            bucket=S3_BUCKET,
            endpoints=S3_ENDPOINTS,
            access_key_id=S3_ACCESS_KEY_ID,
            secret_access_key=S3_SECRET_ACCESS_KEY,
            region=S3_REGION,
            delay=delay,
            bandwidth_bps=bandwidth_bps,
        )


def get_client_backend_name(
    backend: str | None = None,
) -> str:
    """Get the class name of the selected S3 client backend."""
    return _get_backend_class(backend).__name__
