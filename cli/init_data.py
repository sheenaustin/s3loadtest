from __future__ import annotations

"""Init data command - Initialize pre-generated data files on NFS."""

import os
import tempfile
from argparse import Namespace

from s3loadtest.config import DATA_DIR
from s3loadtest.utils import get_deterministic_filename, format_bytes


def cmd_init_data(args: Namespace) -> int:
    """Initialize pre-generated uncompressible data files on NFS.

    Creates shared data files that all workers use. Files are created atomically
    so multiple workers can run this safely - only one will create each file.
    """
    print("Loadtest Data Initialization")
    print("=" * 60)
    print(f"Data directory: {DATA_DIR}")
    print()

    # Create directory if needed
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except Exception as e:
        print(f"Error creating directory {DATA_DIR}: {e}")
        return 1

    # File configurations
    size_configs = [
        (1024, "1kb"),
        (10 * 1024, "10kb"),
        (100 * 1024, "100kb"),
        (1024 * 1024, "1mb"),
        (10 * 1024 * 1024, "10mb"),
        (100 * 1024 * 1024, "100mb"),
    ]

    created_count = 0
    existing_count = 0
    total_size = 0

    for size_bytes, size_name in size_configs:
        filename = get_deterministic_filename(size_bytes, size_name)
        filepath = os.path.join(DATA_DIR, filename)

        # Check if file already exists
        if os.path.exists(filepath):
            # Verify size
            actual_size = os.path.getsize(filepath)
            if actual_size == size_bytes:
                print(f"OK {filename} already exists ({format_bytes(size_bytes)})")
                existing_count += 1
                total_size += size_bytes
            else:
                print(f"FAIL {filename} exists but has incorrect size ({actual_size} != {size_bytes})!")
                return 1
            continue

        # Create file atomically
        print(f"Creating {filename} ({format_bytes(size_bytes)})...")

        # Create temporary file in same directory (for atomic rename)
        try:
            temp_fd, temp_path = tempfile.mkstemp(
                dir=DATA_DIR,
                prefix=".tmp-",
                suffix=".bin"
            )

            # Write uncompressible random data
            chunk_size = 1024 * 1024  # 1MB chunks
            bytes_written = 0

            with os.fdopen(temp_fd, "wb") as f:
                while bytes_written < size_bytes:
                    write_size = min(chunk_size, size_bytes - bytes_written)
                    f.write(os.urandom(write_size))
                    bytes_written += write_size

            # Atomic rename (fails if file already exists from another worker)
            try:
                os.link(temp_path, filepath)
                os.unlink(temp_path)
                print(f"OK Created {filename}")
                created_count += 1
                total_size += size_bytes
            except FileExistsError:
                # Another worker created it first - verify and continue
                os.unlink(temp_path)
                if os.path.exists(filepath) and os.path.getsize(filepath) == size_bytes:
                    print(f"OK {filename} created by another worker")
                    existing_count += 1
                    total_size += size_bytes
                else:
                    print(f"FAIL {filename} created by another worker but has incorrect size!")
                    return 1

        except Exception as e:
            print(f"FAIL Error creating {filename}: {e}")
            try:
                if "temp_path" in locals():
                    os.unlink(temp_path)
            except:
                pass
            return 1

    print()
    print("=" * 60)
    print(f"Summary:")
    print(f"  Created: {created_count}")
    print(f"  Already existed: {existing_count}")
    print(f"  Total: {created_count + existing_count}")
    print(f"  Total size: {format_bytes(total_size)}")
    print()
    print("Data files ready!")
    print()
    print("Note: Files are uncompressible (pure random data) to simulate")
    print("      realistic storage and network load.")
    return 0
