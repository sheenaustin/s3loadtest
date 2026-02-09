"""Key File Management — Thread-safe tracking of created object keys.

Keys are stored in per-size files for easy size-targeted reads::

    loadtest-keys-baseload-0-1MB.txt
    loadtest-keys-baseload-0-100MB.txt
    loadtest-keys-checkpoint-0-10MB.txt

Usage::

    from s3loadtest.keyfiles import append_object_key, get_random_object_key

    append_object_key("baseload", 0, "loadtest/0/1MB/baseload/...", size_label="1MB")
    key = get_random_object_key("baseload", 0, size_label="1MB")
    keys_by_size = get_keys_by_size(0)  # {"1MB": [...], "100MB": [...]}
"""

from __future__ import annotations

import fcntl
import glob
import os
import random
import tempfile as _tempfile
from collections.abc import Sequence
from threading import Lock

from s3loadtest.config import KEY_FILE_DIR


class KeyFileManager:
    """Thread-safe file-based key tracking with proper locking."""

    def __init__(
        self, key_file_dir: str | None = None,
    ) -> None:
        if key_file_dir is None:
            key_file_dir = KEY_FILE_DIR
        self.key_file_dir = key_file_dir
        self._locks: dict[str, Lock] = {}
        self._lock_lock = Lock()

    def _get_filepath(
        self,
        test_name: str,
        worker_id: int,
        size_label: str | None = None,
    ) -> str:
        """Get path to key tracking file.

        Args:
            test_name: Test name.
            worker_id: Worker ID.
            size_label: Size bucket (e.g. '1MB', '100MB').
                If None, uses legacy format without size.
        """
        if size_label:
            return os.path.join(
                self.key_file_dir,
                f"loadtest-keys-{test_name}-{worker_id}"
                f"-{size_label}.txt",
            )
        return os.path.join(
            self.key_file_dir,
            f"loadtest-keys-{test_name}-{worker_id}.txt",
        )

    def _get_lock(self, filepath: str) -> Lock:
        with self._lock_lock:
            if filepath not in self._locks:
                self._locks[filepath] = Lock()
            return self._locks[filepath]

    def _read_all(self, filepath: str) -> list[str]:
        """Read all keys from a single file."""
        if not os.path.exists(filepath):
            return []
        thread_lock = self._get_lock(filepath)
        with thread_lock:
            try:
                with open(filepath) as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                    try:
                        return [
                            line.strip()
                            for line in f
                            if line.strip()
                        ]
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                return []

    def _find_size_files(
        self, test_name: str, worker_id: int,
    ) -> list[tuple[str, str]]:
        """Find all size-specific key files for a test/worker.

        Returns:
            List of (filepath, size_label) tuples.
        """
        pattern = os.path.join(
            self.key_file_dir,
            f"loadtest-keys-{test_name}-{worker_id}-*.txt",
        )
        results: list[tuple[str, str]] = []
        for filepath in sorted(glob.glob(pattern)):
            basename = os.path.basename(filepath)
            # loadtest-keys-baseload-0-10MB.txt → 10MB
            stem = basename[:-4]  # strip .txt
            # Find last hyphen-separated part
            prefix = f"loadtest-keys-{test_name}-{worker_id}-"
            if stem.startswith(prefix[:-1]):
                size_label = stem[len(prefix) - 1:]
                # Handle edge case: stem is prefix without trailing -
                idx = len(f"loadtest-keys-{test_name}-{worker_id}-")
                size_label = stem[idx:]
                if size_label:
                    results.append((filepath, size_label))
        return results

    def append(
        self,
        test_name: str,
        worker_id: int,
        key: str,
        size_label: str | None = None,
    ) -> None:
        """Append object key to tracking file (thread-safe)."""
        filepath = self._get_filepath(
            test_name, worker_id, size_label,
        )
        thread_lock = self._get_lock(filepath)
        with thread_lock:
            try:
                with open(filepath, "a") as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    try:
                        f.write(f"{key}\n")
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass

    def get_random(
        self,
        test_name: str,
        worker_id: int,
        size_label: str | None = None,
    ) -> str | None:
        """Get random key using reservoir sampling (O(1) memory)."""
        filepath = self._get_filepath(
            test_name, worker_id, size_label,
        )
        if not os.path.exists(filepath):
            return None
        thread_lock = self._get_lock(filepath)
        with thread_lock:
            try:
                chosen = None
                with open(filepath) as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                    try:
                        for i, line in enumerate(f, 1):
                            line = line.strip()
                            if line and random.randrange(i) == 0:
                                chosen = line
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                return chosen
            except OSError:
                return None

    def count(
        self,
        test_name: str,
        worker_id: int,
        size_label: str | None = None,
    ) -> int:
        """Count how many keys are tracked."""
        filepath = self._get_filepath(
            test_name, worker_id, size_label,
        )
        if not os.path.exists(filepath):
            return 0
        thread_lock = self._get_lock(filepath)
        with thread_lock:
            try:
                with open(filepath) as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                    try:
                        return sum(
                            1 for line in f if line.strip()
                        )
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                return 0

    def count_all_sizes(
        self, test_name: str, worker_id: int,
    ) -> int:
        """Count total keys across all size files."""
        total = 0
        for filepath, _sl in self._find_size_files(
            test_name, worker_id,
        ):
            total += self.count(test_name, worker_id, _sl)
        # Also check legacy file
        legacy = self.count(test_name, worker_id)
        return total + legacy

    def trim(
        self,
        test_name: str,
        worker_id: int,
        max_keys: int,
        size_label: str | None = None,
    ) -> None:
        """Keep only the last max_keys entries."""
        filepath = self._get_filepath(
            test_name, worker_id, size_label,
        )
        if not os.path.exists(filepath):
            return
        thread_lock = self._get_lock(filepath)
        with thread_lock:
            try:
                with open(filepath) as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    try:
                        lines = [
                            line for line in f if line.strip()
                        ]
                        if len(lines) <= max_keys:
                            return
                        dir_path = os.path.dirname(filepath)
                        fd, temp_path = _tempfile.mkstemp(
                            dir=dir_path, prefix=".keys-",
                        )
                        try:
                            with os.fdopen(fd, "w") as tf:
                                tf.writelines(
                                    lines[-max_keys:]
                                )
                            os.rename(temp_path, filepath)
                        except OSError:
                            try:
                                os.unlink(temp_path)
                            except OSError:
                                pass
                            raise
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass

    def remove_batch(
        self,
        test_name: str,
        worker_id: int,
        keys_to_remove: Sequence[str],
        size_label: str | None = None,
    ) -> None:
        """Remove multiple keys from tracking file(s).

        If size_label is None, searches ALL size files for this
        test/worker (needed by delete test which doesn't know sizes).
        """
        if not keys_to_remove:
            return
        keys_set = set(keys_to_remove)

        if size_label is not None:
            self._remove_from_file(
                self._get_filepath(
                    test_name, worker_id, size_label,
                ),
                keys_set,
            )
            return

        # No size specified — search all files
        # Check legacy file
        legacy = self._get_filepath(test_name, worker_id)
        if os.path.exists(legacy):
            self._remove_from_file(legacy, keys_set)
        # Check all size files
        for filepath, _sl in self._find_size_files(
            test_name, worker_id,
        ):
            self._remove_from_file(filepath, keys_set)

    def _remove_from_file(
        self, filepath: str, keys_set: set[str],
    ) -> None:
        """Remove keys matching the set from a single file."""
        if not os.path.exists(filepath):
            return
        thread_lock = self._get_lock(filepath)
        with thread_lock:
            try:
                with open(filepath, "r+") as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    try:
                        lines = [
                            line for line in f
                            if line.strip()
                            and line.strip() not in keys_set
                        ]
                        f.seek(0)
                        f.truncate()
                        f.writelines(lines)
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass

    def get_all_keys(
        self,
        test_name: str,
        worker_id: int,
        size_label: str | None = None,
    ) -> list[str]:
        """Get all keys from a tracking file."""
        return self._read_all(
            self._get_filepath(
                test_name, worker_id, size_label,
            )
        )

    def get_keys_by_size(
        self,
        test_name: str,
        worker_id: int,
    ) -> dict[str, list[str]]:
        """Get all keys grouped by size label.

        Returns:
            Dict mapping size_label to list of keys.
            Legacy keys (no size) are under "unknown".
        """
        result: dict[str, list[str]] = {}

        # Size-specific files
        for filepath, size_label in self._find_size_files(
            test_name, worker_id,
        ):
            keys = self._read_all(filepath)
            if keys:
                result[size_label] = keys

        # Legacy file (no size in name)
        legacy_keys = self._read_all(
            self._get_filepath(test_name, worker_id)
        )
        if legacy_keys:
            result["unknown"] = legacy_keys

        return result


_key_manager = KeyFileManager()


# --- Module-level convenience functions ---

def append_object_key(
    test_name: str,
    worker_id: int,
    key: str,
    size_label: str | None = None,
) -> None:
    """Append object key to tracking file."""
    _key_manager.append(
        test_name, worker_id, key, size_label,
    )


def get_random_object_key(
    test_name: str,
    worker_id: int,
    size_label: str | None = None,
) -> str | None:
    """Get random key from tracking file."""
    return _key_manager.get_random(
        test_name, worker_id, size_label,
    )


def count_object_keys(
    test_name: str,
    worker_id: int,
    size_label: str | None = None,
) -> int:
    """Count how many keys are tracked."""
    return _key_manager.count(
        test_name, worker_id, size_label,
    )


def trim_object_keys(
    test_name: str,
    worker_id: int,
    max_keys: int,
    size_label: str | None = None,
) -> None:
    """Keep only the last max_keys entries."""
    _key_manager.trim(
        test_name, worker_id, max_keys, size_label,
    )


def remove_object_keys_batch(
    test_name: str,
    worker_id: int,
    keys_to_remove: Sequence[str],
    size_label: str | None = None,
) -> None:
    """Remove multiple keys from tracking file(s)."""
    _key_manager.remove_batch(
        test_name, worker_id, keys_to_remove, size_label,
    )


def get_all_object_keys(
    test_name: str,
    worker_id: int,
    size_label: str | None = None,
) -> list[str]:
    """Get all keys from tracking file."""
    return _key_manager.get_all_keys(
        test_name, worker_id, size_label,
    )


def get_keys_by_size(
    worker_id: int,
    test_names: Sequence[str] | None = None,
) -> dict[str, list[str]]:
    """Get all keys across tests, grouped by size label.

    Args:
        worker_id: Worker ID.
        test_names: Test names to include (default: all common tests).

    Returns:
        Dict mapping size_label to list of keys.
    """
    if test_names is None:
        test_names = [
            "baseload", "checkpoint", "spiky", "elephant",
        ]

    merged: dict[str, list[str]] = {}
    for test in test_names:
        by_size = _key_manager.get_keys_by_size(test, worker_id)
        for size_label, keys in by_size.items():
            if size_label in merged:
                merged[size_label].extend(keys)
            else:
                merged[size_label] = list(keys)

    return merged
