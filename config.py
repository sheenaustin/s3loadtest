"""Configuration — All tunables in one place.

Configuration is loaded from these sources (in priority order):
    1. Environment variables (highest priority)
    2. ``.env`` file in current working directory
    3. ``.env`` file in ``~/.s3loadtest/``
    4. Built-in defaults
"""

from __future__ import annotations

import os
from pathlib import Path


# ---------------------------------------------------------------------------
# Stdlib .env file loader (no external dependency)
# ---------------------------------------------------------------------------

def _load_dotenv() -> None:
    """Load KEY=VALUE pairs from a .env file into ``os.environ``.

    Searches the current working directory first, then
    ``~/.s3loadtest/``. Only sets variables that are not already
    present in the environment (env vars take priority).
    """
    candidates = [
        Path.cwd() / ".env",
        Path.home() / ".s3loadtest" / ".env",
    ]
    for env_path in candidates:
        if env_path.is_file():
            _parse_env_file(env_path)
            return


def _parse_env_file(path: Path) -> None:
    """Parse a .env file and inject into ``os.environ``."""
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                # Strip surrounding quotes
                if (
                    len(value) >= 2
                    and value[0] == value[-1]
                    and value[0] in ('"', "'")
                ):
                    value = value[1:-1]
                # Only set if not already in environment
                if key not in os.environ:
                    os.environ[key] = value
    except OSError:
        pass


# Load .env before reading any configuration
_load_dotenv()


# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_STATS_INTERVAL = 30

# ---------------------------------------------------------------------------
# S3 Connection
# ---------------------------------------------------------------------------
_s3_pool_str = os.environ.get("S3_POOL") or os.environ.get(
    "S3_ENDPOINTS", ""
)
S3_ENDPOINTS: list[str] = [
    ep.strip() for ep in _s3_pool_str.split(",") if ep.strip()
]

S3_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_REGION = os.environ.get("AWS_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# S3 Client Backend
# ---------------------------------------------------------------------------
S3_BACKEND = os.environ.get("S3LOADTEST_BACKEND", "boto3")

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
DATA_DIR = os.environ.get(
    "S3LOADTEST_DATA_DIR", str(Path.cwd() / "data")
)
LOG_DIR = os.environ.get(
    "S3LOADTEST_LOG_DIR", str(Path.cwd() / "logs")
)
TEMP_DIR = os.environ.get("S3LOADTEST_TEMP_DIR", "/tmp")
KEY_FILE_DIR = os.environ.get("S3LOADTEST_KEY_DIR", TEMP_DIR)

WORKERS_FILE = Path(
    os.environ.get(
        "S3LOADTEST_WORKERS_FILE",
        str(Path.cwd() / "workers"),
    )
)

# Remote paths (for SSH-based distributed mode)
REMOTE_CODE_DIR = os.environ.get("S3LOADTEST_REMOTE_CODE_DIR", "")
REMOTE_LOG_DIR = os.environ.get("S3LOADTEST_REMOTE_LOG_DIR", "")

# ---------------------------------------------------------------------------
# Thread Allocation — Based on Target System Load
# ---------------------------------------------------------------------------
CPU_ALLOCATION_PERCENTAGE = 0.90

TEST_TARGET_LOAD: dict[str, float] = {
    "baseload": 0.75,
    "checkpoint": 1.00,
    "heavyread": 1.00,
    "delete": 0.25,
    "elephant": 0.25,
    "listops": 0.25,
    "spiky": 1.00,
}

# ---------------------------------------------------------------------------
# Object Size Configuration
# ---------------------------------------------------------------------------
SIZE_RANGES: dict[str, tuple[int, int]] = {
    "1KB": (1024, 1024),
    "10KB": (10 * 1024, 10 * 1024),
    "100KB": (100 * 1024, 100 * 1024),
    "1MB": (1024 * 1024, 1024 * 1024),
    "10MB": (10 * 1024 * 1024, 10 * 1024 * 1024),
    "100MB": (100 * 1024 * 1024, 100 * 1024 * 1024),
    "1GB": (1024 * 1024 * 1024, 1024 * 1024 * 1024),
    "10GB": (10 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024),
    "100GB": (
        100 * 1024 * 1024 * 1024,
        100 * 1024 * 1024 * 1024,
    ),
}
ALL_SIZES: list[str] = [
    "1KB", "10KB", "100KB", "1MB", "10MB", "100MB",
]
CHECKPOINT_SIZES: list[str] = ["10MB", "100MB"]

# ---------------------------------------------------------------------------
# Test Behavior Configuration
# ---------------------------------------------------------------------------
MAX_OBJECTS_PER_WORKER = 500

# Retry configuration
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 0.1  # seconds
RETRY_MAX_DELAY = 30  # seconds

# ---------------------------------------------------------------------------
# Proxy Configuration (optional — for s3pool backend only)
# ---------------------------------------------------------------------------
PROXY_BINARY_PATH = os.environ.get("S3POOL_BINARY", "s3pool")
PROXY_LISTEN_ADDR = os.environ.get(
    "S3LOADTEST_PROXY_ADDR", "0.0.0.0:18080"
)
PROXY_PORT = PROXY_LISTEN_ADDR.split(":")[-1]
PROXY_LOG_FILE = "/tmp/s3pool-proxy.log"
PROXY_STARTUP_TIMEOUT = 15
PROXY_MAX_PARALLEL_SSH = 15
