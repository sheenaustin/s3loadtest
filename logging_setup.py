"""Centralized Logging Setup for Load Tests.

Usage::

    from s3loadtest.logging_setup import setup_logging, get_logger

    setup_logging(level="DEBUG")
    logger = get_logger(test_name="baseload", worker_id=0)
    logger.info("Test started")
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from typing import Any

from s3loadtest.config import DEFAULT_LOG_LEVEL


class LoadTestFormatter(logging.Formatter):
    """Custom formatter with structured fields for load test logging."""

    COLORS: dict[str, str] = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
        "RESET": "\033[0m",
    }

    def __init__(self, *, use_color: bool = True) -> None:
        self.use_color = use_color and sys.stderr.isatty()
        super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record with test context."""
        test_name = getattr(record, "test_name", "-")
        worker_id = getattr(record, "worker_id", "-")
        op_type = getattr(record, "op_type", "")

        timestamp = datetime.fromtimestamp(
            record.created,
        ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        level = record.levelname
        if self.use_color and level in self.COLORS:
            level_str = (
                f"{self.COLORS[level]}"
                f"{level:8s}"
                f"{self.COLORS['RESET']}"
            )
        else:
            level_str = f"{level:8s}"

        if test_name != "-" or worker_id != "-":
            context = f"[{test_name}:W{worker_id}]"
        else:
            context = ""

        op_tag = f"[{op_type}] " if op_type else ""
        message = record.getMessage()

        return (
            f"{timestamp} {level_str} "
            f"{context:20s} {op_tag}{message}"
        )


class JsonFormatter(logging.Formatter):
    """JSON formatter for machine parsing."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON."""
        log_data: dict[str, Any] = {
            "ts": datetime.fromtimestamp(
                record.created,
            ).isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
            "test": getattr(record, "test_name", None),
            "worker": getattr(record, "worker_id", None),
            "op": getattr(record, "op_type", None),
        }
        log_data = {
            k: v for k, v in log_data.items() if v is not None
        }
        return json.dumps(log_data)


class ContextLogger(logging.LoggerAdapter):
    """Logger adapter that adds test context to all messages."""

    def process(
        self,
        msg: str,
        kwargs: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """Add context fields to the log record."""
        extra = kwargs.get("extra", {})
        extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs


def setup_logging(
    *,
    level: str | None = None,
    log_file: str | None = None,
) -> logging.Logger:
    """Configure centralized logging for loadtest.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR).
        log_file: Optional file path to write logs to.

    Returns:
        Configured logger instance.
    """
    if level is None:
        level = os.environ.get(
            "LOADTEST_LOG_LEVEL", DEFAULT_LOG_LEVEL,
        ).upper()
    numeric_level = getattr(logging, level, logging.INFO)

    logger = logging.getLogger("loadtest")
    logger.setLevel(numeric_level)
    logger.handlers.clear()

    use_json = (
        os.environ.get("LOADTEST_LOG_JSON", "0") == "1"
    )

    if use_json:
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = LoadTestFormatter(use_color=True)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file is None:
        log_file = os.environ.get("LOADTEST_LOG_FILE")

    if log_file:
        file_handler = logging.FileHandler(log_file)
        if use_json:
            file_handler.setFormatter(formatter)
        else:
            file_handler.setFormatter(
                LoadTestFormatter(use_color=False),
            )
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def get_logger(
    *,
    test_name: str | None = None,
    worker_id: int | None = None,
) -> ContextLogger:
    """Get a logger with optional test context.

    Args:
        test_name: Name of the test.
        worker_id: Worker ID number.

    Returns:
        ContextLogger with test context attached.
    """
    base_logger = logging.getLogger("loadtest")

    if not base_logger.handlers:
        setup_logging()

    extra: dict[str, Any] = {}
    if test_name is not None:
        extra["test_name"] = test_name
    if worker_id is not None:
        extra["worker_id"] = worker_id

    return ContextLogger(base_logger, extra)
