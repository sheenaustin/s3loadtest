#!/usr/bin/env python3
"""Entry point for s3loadtest package.

Usage::

    s3loadtest start baseload --duration 5m
    s3loadtest status
    s3loadtest stop all
    s3loadtest run baseload --worker-id 0 --duration 5m
"""

from __future__ import annotations

import argparse
import sys

from s3loadtest import __version__


def main() -> int:
    """Parse CLI arguments and dispatch to the appropriate command."""
    parser = argparse.ArgumentParser(
        description="S3 Load Testing Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available tests:
  all, baseload, checkpoint, heavyread, delete, elephant, listops, spiky

Commands:
  init-data   Initialize pre-generated data files (run once)
  start       Start load tests on workers
  stop        Stop running load tests
  status      Show status of running tests
  cleanup     Clean up test objects from S3
  run         Run a test locally (single machine or worker mode)

Examples:
  s3loadtest start baseload --duration 5m
  s3loadtest start all --duration 1h --workers 10
  s3loadtest run baseload --worker-id 0 --duration 5m
  s3loadtest status
  s3loadtest stop all
  s3loadtest cleanup --prefix loadtest/
""",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=[
            "init-data",
            "start",
            "stop",
            "status",
            "cleanup",
            "run",
        ],
        help="Command to execute",
    )
    parser.add_argument(
        "test_name",
        nargs="?",
        help=(
            "Test name: all, baseload, checkpoint, heavyread, "
            "delete, elephant, listops, spiky"
        ),
    )
    parser.add_argument(
        "--duration",
        type=str,
        default=None,
        help="Test duration (e.g. 5m, 1h, 2d, 1w)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of workers to use (default: all)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level",
    )
    parser.add_argument(
        "--no-proxy",
        action="store_true",
        help="Don't check/start proxies before tests",
    )
    parser.add_argument(
        "--force-restart-proxy",
        action="store_true",
        help="Force restart all proxies before tests",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="loadtest/",
        help="S3 prefix for cleanup (default: loadtest/)",
    )

    # Run command arguments
    parser.add_argument(
        "--worker-id",
        type=int,
        default=0,
        help="Worker ID (for run command)",
    )
    parser.add_argument(
        "--stats-interval",
        type=int,
        default=30,
        help="Stats logging interval in seconds",
    )

    # Legacy backward compat for SSH workers
    parser.add_argument(
        "--run",
        help=argparse.SUPPRESS,
    )

    args = parser.parse_args()

    # Support legacy --run invocation from older workers
    if args.run:
        args.command = "run"
        args.test_name = args.run

    if not args.command:
        parser.print_help()
        return 0

    from s3loadtest.cli import (
        cmd_cleanup,
        cmd_init_data,
        cmd_run,
        cmd_start,
        cmd_status,
        cmd_stop,
    )

    commands = {
        "init-data": cmd_init_data,
        "start": cmd_start,
        "stop": cmd_stop,
        "status": cmd_status,
        "cleanup": cmd_cleanup,
        "run": cmd_run,
    }

    try:
        return commands[args.command](args) or 0
    except KeyboardInterrupt:
        print("\nInterrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
