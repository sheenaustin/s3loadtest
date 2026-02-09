from __future__ import annotations

"""Elephant Flow Test: Connection exhaustion via slow-trickle I/O.

Opens 1000+ concurrent connections to the object store and trickles
data at ~100 bytes/sec per connection. Each connection holds a
server-side file descriptor and TCP buffer for minutes to hours.

This kills the object store differently from throughput tests:
- Exhausts server connection pools / file descriptor limits
- Fills server-side TCP buffer memory
- Triggers timeout/keepalive pressure on the server
- A 1MB object at 100 B/s = ~2.8 hours per connection
- 1000 connections = 1000 server-side FDs held simultaneously

When a connection finishes or dies, a new one is immediately
opened to maintain pressure.
"""

import http.client
import os
import random
import ssl
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from s3loadtest.config import (
    ALL_SIZES,
    ELEPHANT_CONNECTIONS,
    ELEPHANT_TRICKLE_BPS,
    S3_BUCKET,
    S3_ENDPOINTS,
    SIZE_RANGES,
)
from s3loadtest.keyfiles import get_keys_by_size
from s3loadtest.tests.base import LoadTest
from s3loadtest.utils import format_duration, generate_random_suffix

_KEY_SOURCES = ["baseload", "checkpoint", "spiky", "firehose"]

# Unverified SSL context for self-signed certs
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE


class ElephantFlowTest(LoadTest):
    """Elephant Flow: Connection exhaustion via slow-trickle I/O.

    Opens ELEPHANT_CONNECTIONS concurrent connections and trickles
    data painfully slowly to hold server resources hostage.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        # Override concurrency with elephant-specific connection count
        self.concurrency = max(self.concurrency, ELEPHANT_CONNECTIONS)
        self._active_conns = 0
        self._active_lock = Lock()
        self._total_trickled = 0
        self._conn_errors = 0
        self._server_resets = 0

    def _incr_active(self, delta: int) -> None:
        with self._active_lock:
            self._active_conns += delta

    def _pick_endpoint(self) -> tuple[str, int, bool]:
        """Pick a random S3 endpoint, return (host, port, use_ssl)."""
        url = random.choice(S3_ENDPOINTS)
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        use_ssl = parsed.scheme == "https"
        return host, port, use_ssl

    def _slow_write(self) -> tuple[int, str]:
        """Open a connection and trickle a PUT byte by byte.

        Returns (bytes_sent, error_or_empty).
        """
        host, port, use_ssl = self._pick_endpoint()

        # Pick a size — determines how long we hold the connection
        size_name = random.choice(ALL_SIZES)
        size_bytes = SIZE_RANGES[size_name][0]
        suffix = generate_random_suffix(16)
        key = (
            f"loadtest/{self.worker_id}/1B/elephant"
            f"/{int(time.time()*1000000)}/{suffix}"
        )
        path = f"/{S3_BUCKET}/{key}"

        # Send up to 10KB of actual data but very slowly.
        actual_bytes = min(size_bytes, 10240)

        try:
            if use_ssl:
                conn = http.client.HTTPSConnection(
                    host, port, timeout=600, context=_ssl_ctx,
                )
            else:
                conn = http.client.HTTPConnection(
                    host, port, timeout=600,
                )

            self._incr_active(1)

            # Manually send headers + trickle body
            conn.putrequest("PUT", path)
            conn.putheader("Content-Length", str(actual_bytes))
            conn.putheader("Content-Type", "application/octet-stream")
            conn.endheaders()

            # Trickle the body
            sent = 0
            chunk_size = max(1, ELEPHANT_TRICKLE_BPS // 10)
            sleep_interval = chunk_size / ELEPHANT_TRICKLE_BPS

            while sent < actual_bytes and not self.stop_event.is_set():
                to_send = min(chunk_size, actual_bytes - sent)
                conn.sock.sendall(os.urandom(to_send))
                sent += to_send
                time.sleep(sleep_interval)

            # Read response (may fail if server gave up)
            resp = conn.getresponse()
            resp.read()
            conn.close()

            self._incr_active(-1)
            return sent, ""

        except (ConnectionResetError, BrokenPipeError):
            self._incr_active(-1)
            with self._active_lock:
                self._server_resets += 1
            return 0, "reset"
        except Exception as e:
            self._incr_active(-1)
            return 0, str(e)

    def _slow_read(self) -> tuple[int, str]:
        """Open a connection and read a GET response byte by byte.

        Returns (bytes_read, error_or_empty).
        """
        host, port, use_ssl = self._pick_endpoint()

        # Find a key to read
        keys_by_size = get_keys_by_size(self.worker_id, _KEY_SOURCES)
        all_keys: list[str] = []
        for keys in keys_by_size.values():
            all_keys.extend(keys)

        if not all_keys:
            return self._slow_write()

        key = random.choice(all_keys)
        path = f"/{S3_BUCKET}/{key}"

        try:
            if use_ssl:
                conn = http.client.HTTPSConnection(
                    host, port, timeout=600, context=_ssl_ctx,
                )
            else:
                conn = http.client.HTTPConnection(
                    host, port, timeout=600,
                )

            self._incr_active(1)

            conn.request("GET", path)
            resp = conn.getresponse()

            if resp.status != 200:
                resp.read()
                conn.close()
                self._incr_active(-1)
                if resp.status == 404:
                    return 0, ""
                return 0, f"HTTP {resp.status}"

            # Trickle-read the response
            total_read = 0
            chunk_size = max(1, ELEPHANT_TRICKLE_BPS // 10)
            sleep_interval = chunk_size / ELEPHANT_TRICKLE_BPS

            while not self.stop_event.is_set():
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                total_read += len(chunk)
                time.sleep(sleep_interval)

            conn.close()
            self._incr_active(-1)
            return total_read, ""

        except (ConnectionResetError, BrokenPipeError):
            self._incr_active(-1)
            with self._active_lock:
                self._server_resets += 1
            return 0, "reset"
        except Exception as e:
            self._incr_active(-1)
            return 0, str(e)

    def _elephant_worker(self) -> tuple[int, int, int]:
        """Continuously open slow connections until stopped.

        Returns (total_bytes, total_ops, total_errors).
        """
        total_bytes = 0
        total_ops = 0
        total_errors = 0

        while not self.stop_event.is_set() and self.should_continue():
            if random.random() < 0.5:
                nbytes, err = self._slow_write()
            else:
                nbytes, err = self._slow_read()

            if err:
                total_errors += 1
            else:
                total_ops += 1
                total_bytes += nbytes

        return total_bytes, total_ops, total_errors

    def run(self) -> None:
        self.log(
            f"Starting Elephant Flow (connection exhaustion) "
            f"with {self.concurrency} slow connections "
            f"at {ELEPHANT_TRICKLE_BPS} bytes/sec each"
        )
        if self.duration_seconds:
            self.log(f"Running for {format_duration(self.duration_seconds)}")

        thread_results: list[tuple[int, int, int]] = []
        results_lock = Lock()

        def wrapper() -> None:
            result = self._elephant_worker()
            with results_lock:
                thread_results.append(result)

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = [
                executor.submit(wrapper)
                for _ in range(self.concurrency)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.update_stats(worker_failures=1)
                    self.log(f"Worker thread error: {e}")

        total_bytes = sum(r[0] for r in thread_results)
        total_ops = sum(r[1] for r in thread_results)
        total_errors = sum(r[2] for r in thread_results)

        self.update_stats(
            ops=total_ops, bytes=total_bytes, errors=total_errors,
        )

        elapsed = time.time() - self.start_time
        self.log(
            f"Elephant complete: {total_ops:,} slow transfers, "
            f"{total_bytes:,} bytes trickled, "
            f"{total_errors} errors, "
            f"{self._server_resets} server resets "
            f"in {elapsed:.0f}s"
        )
        if total_ops > 0 and elapsed > 0:
            self.log(
                f"  Avg connection hold time: "
                f"{elapsed * self.concurrency / total_ops:.1f}s"
            )
