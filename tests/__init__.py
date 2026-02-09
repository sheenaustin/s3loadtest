from __future__ import annotations

# Test classes registry

from s3loadtest.tests.baseload import BaseLoadTest
from s3loadtest.tests.spiky import SpikyBaseLoadTest
from s3loadtest.tests.checkpoint import CheckpointLoadTest
from s3loadtest.tests.heavyread import HeavyReadTest
from s3loadtest.tests.delete import DeleteLoadTest
from s3loadtest.tests.elephant import ElephantFlowTest
from s3loadtest.tests.firehose import FirehoseTest
from s3loadtest.tests.listops import ListOpsTest
from s3loadtest.tests.metastorm import MetastormTest

TEST_CLASSES: dict[str, type] = {
    "baseload": BaseLoadTest,
    "checkpoint": CheckpointLoadTest,
    "heavyread": HeavyReadTest,
    "delete": DeleteLoadTest,
    "elephant": ElephantFlowTest,
    "firehose": FirehoseTest,
    "listops": ListOpsTest,
    "metastorm": MetastormTest,
    "spiky": SpikyBaseLoadTest,
}

__all__ = [
    "TEST_CLASSES",
    "BaseLoadTest",
    "SpikyBaseLoadTest",
    "CheckpointLoadTest",
    "HeavyReadTest",
    "DeleteLoadTest",
    "ElephantFlowTest",
    "FirehoseTest",
    "ListOpsTest",
    "MetastormTest",
]
