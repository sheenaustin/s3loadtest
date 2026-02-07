"""Key File Management — Thread-safe tracking of created object keys.

Uses file-based tracking with fcntl locking to track object keys
created by load tests, allowing random selection for read operations.

Usage::

    from s3loadtest.keyfiles import append_object_key, get_random_object_key

    append_object_key("baseload", worker_id, "path/to/object.bin")
    key = get_random_object_key("baseload", worker_id)
"""

from __future__ import annotations

import fcntl
import os
import random
import tempfile as _tempfile
from collections.abc import Sequence
from threading import Lock

from s3loadtest.config import KEY_FILE_DIR


class KeyFileManager:
    """Thread-safe file-based key tracking with proper locking.

    Uses fcntl.flock for file locking to prevent race conditions when
    multiple threads/processes access the same key file.
    """

    def __init__(
        self, key_file_dir: str | None = None,
    ) -> None:
        if key_file_dir is None:
            key_file_dir = KEY_FILE_DIR
        self.key_file_dir = key_file_dir
        self._locks: dict[str, Lock] = {}
        self._lock_lock = Lock()

    def _get_filepath(
        self, test_name: str, worker_id: int,
    ) -> str:
        """Get path to key tracking file for this test/worker."""
        return os.path.join(
            self.key_file_dir,
            f"loadtest-keys-{test_name}-{worker_id}.txt",
        )

    def _get_lock(self, filepath: str) -> Lock:
        """Get or create a threading lock for a filepath."""
        with self._lock_lock:
            if filepath not in self._locks:
                self._locks[filepath] = Lock()
            return self._locks[filepath]

    def append(
        self, test_name: str, worker_id: int, key: str,
    ) -> None:
        """Append object key to tracking file (thread-safe).

        Args:
            test_name: Test name.
            worker_id: Worker ID.
            key: Object key to track.
        """
        filepath = self._get_filepath(test_name, worker_id)
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

    def append_batch(
        self,
        test_name: str,
        worker_id: int,
        keys: Sequence[str],
    ) -> None:
        """Append multiple keys efficiently.

        Args:
            test_name: Test name.
            worker_id: Worker ID.
            keys: Object keys to track.
        """
        if not keys:
            return
        filepath = self._get_filepath(test_name, worker_id)
        thread_lock = self._get_lock(filepath)

        with thread_lock:
            try:
                with open(filepath, "a") as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    try:
                        f.writelines(
                            f"{key}\n" for key in keys
                        )
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass

    def get_random(
        self, test_name: str, worker_id: int,
    ) -> str | None:
        """Get random key using reservoir sampling (O(1) memory).

        Args:
            test_name: Test name.
            worker_id: Worker ID.

        Returns:
            Random key or None if no keys tracked.
        """
        filepath = self._get_filepath(test_name, worker_id)
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
        self, test_name: str, worker_id: int,
    ) -> int:
        """Count how many keys are tracked.

        Args:
            test_name: Test name.
            worker_id: Worker ID.

        Returns:
            Number of tracked keys.
        """
        filepath = self._get_filepath(test_name, worker_id)
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

    def trim(
        self,
        test_name: str,
        worker_id: int,
        max_keys: int,
    ) -> None:
        """Keep only the last max_keys entries.

        Args:
            test_name: Test name.
            worker_id: Worker ID.
            max_keys: Maximum number of keys to retain.
        """
        filepath = self._get_filepath(test_name, worker_id)
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

    def remove(
        self,
        test_name: str,
        worker_id: int,
        key_to_remove: str,
    ) -> None:
        """Remove a specific key from the tracking file.

        Args:
            test_name: Test name.
            worker_id: Worker ID.
            key_to_remove: Key to remove.
        """
        filepath = self._get_filepath(test_name, worker_id)
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
                            and line.strip() != key_to_remove
                        ]
                        f.seek(0)
                        f.truncate()
                        f.writelines(lines)
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass

    def remove_batch(
        self,
        test_name: str,
        worker_id: int,
        keys_to_remove: Sequence[str],
    ) -> None:
        """Remove multiple keys efficiently.

        Args:
            test_name: Test name.
            worker_id: Worker ID.
            keys_to_remove: Keys to remove.
        """
        if not keys_to_remove:
            return
        filepath = self._get_filepath(test_name, worker_id)
        if not os.path.exists(filepath):
            return

        keys_set = set(keys_to_remove)
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
        self, test_name: str, worker_id: int,
    ) -> list[str]:
        """Get all keys (use sparingly — loads entire file).

        Args:
            test_name: Test name.
            worker_id: Worker ID.

        Returns:
            List of all tracked keys.
        """
        filepath = self._get_filepath(test_name, worker_id)
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


_key_manager = KeyFileManager()


def get_keyfile_path(
    test_name: str, worker_id: int,
) -> str:
    """Get path to key tracking file for this test/worker."""
    return _key_manager._get_filepath(test_name, worker_id)


def append_object_key(
    test_name: str, worker_id: int, key: str,
) -> None:
    """Append object key to tracking file."""
    _key_manager.append(test_name, worker_id, key)


def get_random_object_key(
    test_name: str, worker_id: int,
) -> str | None:
    """Get random key from tracking file."""
    return _key_manager.get_random(test_name, worker_id)


def count_object_keys(
    test_name: str, worker_id: int,
) -> int:
    """Count how many keys are tracked."""
    return _key_manager.count(test_name, worker_id)


def trim_object_keys(
    test_name: str, worker_id: int, max_keys: int,
) -> None:
    """Keep only the last max_keys entries."""
    _key_manager.trim(test_name, worker_id, max_keys)


def remove_object_key(
    test_name: str, worker_id: int, key_to_remove: str,
) -> None:
    """Remove a specific key from the tracking file."""
    _key_manager.remove(test_name, worker_id, key_to_remove)


def remove_object_keys_batch(
    test_name: str,
    worker_id: int,
    keys_to_remove: Sequence[str],
) -> None:
    """Remove multiple keys from the tracking file."""
    _key_manager.remove_batch(
        test_name, worker_id, keys_to_remove,
    )


def get_all_object_keys(
    test_name: str, worker_id: int,
) -> list[str]:
    """Get all keys from tracking file."""
    return _key_manager.get_all_keys(test_name, worker_id)
