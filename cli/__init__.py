"""CLI commands for s3loadtest."""

from __future__ import annotations

from s3loadtest.cli.cleanup import cmd_cleanup
from s3loadtest.cli.init_data import cmd_init_data
from s3loadtest.cli.run import cmd_run
from s3loadtest.cli.start import cmd_start
from s3loadtest.cli.status import cmd_status
from s3loadtest.cli.stop import cmd_stop

__all__ = [
    "cmd_cleanup",
    "cmd_init_data",
    "cmd_run",
    "cmd_start",
    "cmd_status",
    "cmd_stop",
]
