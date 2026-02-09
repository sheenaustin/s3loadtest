"""S3 client backends for load testing.

Available clients:
    S3ClientBoto3      - Pure boto3 (default, works everywhere)
    S3ClientRustProxy  - Routes through s3pool Rust proxy (fastest)
    S3ClientMinio      - MinIO Python SDK (optional, requires minio package)
"""

from __future__ import annotations

import atexit
import http.client
import itertools
import os
import random
import shutil
import socket
import subprocess
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
from base64 import b64encode
from hashlib import md5
from pathlib import Path
from typing import Any
from urllib.parse import quote as url_quote

import boto3
import urllib3
from botocore.config import Config

# Suppress SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_VERIFY_SSL = os.environ.get("S3_VERIFY_SSL", "false").lower() in (
    "true",
    "1",
    "yes",
)


class S3ClientBoto3:
    """Pure boto3 S3 client with endpoint rotation.

    Rotates across all S3 endpoints per operation to avoid
    MinIO per-connection throttling. Clients are cached per
    endpoint to reuse sessions while still distributing load.
    """

    _counter = itertools.count()

    def __init__(
        self,
        *,
        bucket: str,
        endpoints: list[str],
        access_key_id: str,
        secret_access_key: str,
        region: str,
        delay: float = 0,
        bandwidth_bps: int = 0,
    ) -> None:
        self.bucket = bucket
        self.endpoints = endpoints
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region = region
        self.delay = delay
        self._clients: dict[str, Any] = {}

    def _get_client(self) -> Any:
        """Get boto3 client, rotating across endpoints."""
        if not self.endpoints:
            raise RuntimeError("No S3 endpoints configured")
        idx = next(S3ClientBoto3._counter) % len(self.endpoints)
        endpoint = self.endpoints[idx]
        if endpoint not in self._clients:
            self._clients[endpoint] = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                verify=_VERIFY_SSL,
                config=Config(
                    retries={"max_attempts": 3},
                    connect_timeout=10,
                    read_timeout=300,
                ),
            )
        return self._clients[endpoint]

    def put_object(self, key: str, data: bytes) -> dict:
        """Upload object."""
        if self.delay:
            time.sleep(self.delay)
        client = self._get_client()
        return client.put_object(
            Bucket=self.bucket, Key=key, Body=data
        )

    def get_object(self, key: str) -> bytes:
        """Download object."""
        if self.delay:
            time.sleep(self.delay)
        client = self._get_client()
        response = client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def delete_object(self, key: str) -> dict:
        """Delete single object."""
        if self.delay:
            time.sleep(self.delay)
        client = self._get_client()
        return client.delete_object(Bucket=self.bucket, Key=key)

    def delete_objects(self, keys: list[str]) -> dict:
        """Batch delete up to 1000 objects.

        Args:
            keys: List of object keys to delete.

        Returns:
            Dict with 'Deleted' and 'Errors' lists.
        """
        if self.delay:
            time.sleep(self.delay)
        client = self._get_client()
        objects = [{"Key": k} for k in keys]
        response = client.delete_objects(
            Bucket=self.bucket,
            Delete={"Objects": objects},
        )
        return {
            "Deleted": response.get("Deleted", []),
            "Errors": response.get("Errors", []),
        }

    def head_object(self, key: str) -> dict:
        """Get object metadata."""
        if self.delay:
            time.sleep(self.delay)
        client = self._get_client()
        return client.head_object(Bucket=self.bucket, Key=key)

    def list_objects(
        self,
        prefix: str = "",
        max_keys: int = 1000,
        continuation_token: str | None = None,
    ) -> dict:
        """List objects using ListObjectsV2."""
        if self.delay:
            time.sleep(self.delay)
        client = self._get_client()
        params: dict[str, Any] = {
            "Bucket": self.bucket,
            "Prefix": prefix,
            "MaxKeys": max_keys,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        return client.list_objects_v2(**params)


class S3ClientMinio:
    """MinIO Python SDK client.

    Requires the ``minio`` package to be installed. Uses minio-py
    for all operations; works with any S3-compatible endpoint.
    """

    def __init__(
        self,
        *,
        bucket: str,
        endpoints: list[str],
        access_key_id: str,
        secret_access_key: str,
        region: str,
        delay: float = 0,
        bandwidth_bps: int = 0,
    ) -> None:
        """Initialize minio-py client.

        Args:
            bucket: S3 bucket name.
            endpoints: List of S3 endpoint URLs (uses first one).
            access_key_id: AWS access key ID.
            secret_access_key: AWS secret access key.
            region: AWS region.
            delay: Artificial delay in seconds per operation.
            bandwidth_bps: Bandwidth limit in bytes/sec (unused).
        """
        try:
            from minio import Minio
            from urllib3 import PoolManager
        except ImportError as exc:
            raise ImportError(
                "minio package not installed. "
                "Run: pip install minio"
            ) from exc

        self.bucket = bucket
        self.delay = delay

        endpoint_url = random.choice(endpoints) if endpoints else None
        parsed = urllib.parse.urlparse(endpoint_url)
        endpoint_host = parsed.netloc
        use_secure = parsed.scheme == "https"

        http_client = PoolManager(
            timeout=300,
            cert_reqs="CERT_REQUIRED" if _VERIFY_SSL else "CERT_NONE",
            retries=3,
        )

        self.client = Minio(
            endpoint_host,
            access_key=access_key_id,
            secret_key=secret_access_key,
            secure=use_secure,
            http_client=http_client,
        )

    def put_object(self, key: str, data: bytes) -> Any:
        """Upload object."""
        if self.delay:
            time.sleep(self.delay)
        from io import BytesIO

        data_stream = BytesIO(data)
        return self.client.put_object(
            self.bucket, key, data_stream, len(data)
        )

    def get_object(self, key: str) -> bytes:
        """Download object."""
        if self.delay:
            time.sleep(self.delay)
        response = self.client.get_object(self.bucket, key)
        data = response.read()
        response.close()
        response.release_conn()
        return data

    def delete_object(self, key: str) -> Any:
        """Delete single object."""
        if self.delay:
            time.sleep(self.delay)
        return self.client.remove_object(self.bucket, key)

    def delete_objects(self, keys: list[str]) -> dict:
        """Batch delete objects.

        Args:
            keys: List of object keys to delete.

        Returns:
            Dict with 'Deleted' and 'Errors' lists (boto3-style).
        """
        if self.delay:
            time.sleep(self.delay)
        from minio.deleteobjects import DeleteObject

        delete_list = [DeleteObject(key) for key in keys]
        errors = list(
            self.client.remove_objects(self.bucket, delete_list)
        )
        deleted = [
            {"Key": key}
            for key in keys
            if not any(e.name == key for e in errors)
        ]
        error_list = [
            {
                "Key": e.name,
                "Code": e.code,
                "Message": e.message,
            }
            for e in errors
        ]
        return {"Deleted": deleted, "Errors": error_list}

    def head_object(self, key: str) -> dict:
        """Get object metadata."""
        if self.delay:
            time.sleep(self.delay)
        stat = self.client.stat_object(self.bucket, key)
        return {
            "ContentLength": stat.size,
            "LastModified": stat.last_modified,
            "ETag": stat.etag,
        }

    def list_objects(
        self,
        prefix: str = "",
        max_keys: int = 1000,
        continuation_token: str | None = None,
    ) -> dict:
        """List objects, returning boto3-style response dict."""
        if self.delay:
            time.sleep(self.delay)

        start_after = continuation_token if continuation_token else None
        object_iter = self.client.list_objects(
            self.bucket,
            prefix=prefix,
            recursive=True,
            start_after=start_after,
        )

        objects: list[dict] = []
        is_truncated = False
        next_token: str | None = None

        for i, obj in enumerate(object_iter):
            if i >= max_keys:
                is_truncated = True
                next_token = obj.object_name
                break
            objects.append({
                "Key": obj.object_name,
                "Size": obj.size,
                "LastModified": obj.last_modified,
                "ETag": obj.etag,
            })

        response: dict[str, Any] = {
            "Contents": objects,
            "IsTruncated": is_truncated,
            "KeyCount": len(objects),
            "MaxKeys": max_keys,
            "Prefix": prefix,
        }
        if next_token:
            response["NextContinuationToken"] = next_token
        if continuation_token:
            response["ContinuationToken"] = continuation_token
        return response

    def close(self) -> None:
        """Close connections (no-op for minio-py)."""


class _RustProxyManager:
    """Singleton that manages the s3pool Rust proxy process lifecycle.

    Thread-safe: the first ``S3ClientRustProxy`` to be constructed
    triggers proxy startup; all subsequent instances reuse the same
    proxy process. The proxy is terminated via atexit on interpreter
    shutdown.
    """

    _instance: _RustProxyManager | None = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._proc: subprocess.Popen | None = None
        self._port: int | None = None
        self._started: bool = False

    @classmethod
    def get_instance(cls) -> _RustProxyManager:
        """Get or create the singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def ensure_running(self, port: int = 18080) -> None:
        """Start the proxy if not already running on the given port."""
        with self._lock:
            if (
                self._started
                and self._proc
                and self._proc.poll() is None
            ):
                return
            if self._is_port_open(port):
                self._started = True
                self._port = port
                return

            binary = self._find_binary()
            env_dir = self._find_env_dir()
            log_file = Path("/tmp") / f"s3pool-proxy-{port}.log"
            cmd = [
                binary,
                "--insecure",
                "--log-level",
                "error",
                "proxy",
                "--listen",
                f"127.0.0.1:{port}",
            ]
            with open(log_file, "w") as lf:
                self._proc = subprocess.Popen(
                    cmd,
                    stdout=lf,
                    stderr=subprocess.STDOUT,
                    cwd=str(env_dir),
                    start_new_session=True,
                )
            if not self._wait_for_ready(port, timeout=15):
                raise RuntimeError(
                    f"s3pool proxy failed to start on port {port}. "
                    f"Check {log_file} for details."
                )
            self._port = port
            self._started = True
            atexit.register(self.shutdown)

    def shutdown(self) -> None:
        """Terminate the proxy subprocess."""
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=3)
        self._started = False

    @staticmethod
    def _is_port_open(port: int) -> bool:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect(("127.0.0.1", port))
            s.close()
            return True
        except (ConnectionRefusedError, OSError):
            return False

    @staticmethod
    def _wait_for_ready(port: int, *, timeout: int = 15) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1)
                s.connect(("127.0.0.1", port))
                s.close()
                return True
            except (ConnectionRefusedError, OSError):
                time.sleep(0.05)
        return False

    @staticmethod
    def _find_binary() -> str:
        """Locate the s3pool binary.

        Search order:
            1. ``S3POOL_BINARY`` environment variable
            2. ``s3pool`` on ``PATH`` via ``shutil.which``
            3. Common local build paths

        Raises:
            FileNotFoundError: If the binary cannot be found.
        """
        custom = os.environ.get("S3POOL_BINARY")
        if custom and Path(custom).is_file():
            return custom

        on_path = shutil.which("s3pool")
        if on_path:
            return on_path

        # Search common local build paths
        for candidate in (
            Path("/exp/local/s3pool/target/release/s3pool"),
            Path("/exp/local/s3pool-v2/target/release/s3pool"),
            Path.home() / "s3pool" / "target" / "release" / "s3pool",
        ):
            if candidate.is_file():
                return str(candidate)

        raise FileNotFoundError(
            "s3pool binary not found. Set the S3POOL_BINARY "
            "environment variable or ensure 's3pool' is on PATH."
        )

    @staticmethod
    def _find_env_dir() -> Path:
        """Locate directory containing .env for s3pool.

        Search order:
            1. ``S3LOADTEST_ENV_DIR`` environment variable
            2. Current working directory
            3. ``~/.s3loadtest/``

        Raises:
            FileNotFoundError: If no .env file can be found.
        """
        env_dir = os.environ.get("S3LOADTEST_ENV_DIR")
        if env_dir:
            p = Path(env_dir)
            if (p / ".env").is_file():
                return p

        cwd = Path.cwd()
        if (cwd / ".env").is_file():
            return cwd

        home_dir = Path.home() / ".s3loadtest"
        if (home_dir / ".env").is_file():
            return home_dir

        raise FileNotFoundError(
            ".env file not found. Set S3LOADTEST_ENV_DIR or "
            "place a .env file in the current directory or "
            "~/.s3loadtest/"
        )


class S3ClientRustProxy:
    """S3 client that routes through the local s3pool Rust proxy.

    The Rust proxy handles TLS, SigV4 signing, connection pooling,
    and load balancing across all S3 backends. This client speaks
    plain HTTP to localhost, making it the fastest option with the
    lowest Python overhead.

    Uses only stdlib (``http.client``) — zero additional pip
    dependencies. Thread safety: each instance has its own HTTP
    connection; create one per thread.
    """

    def __init__(
        self,
        *,
        bucket: str,
        proxy_port: int | None = None,
        delay: float = 0,
        bandwidth_bps: int = 0,
    ) -> None:
        """Initialize proxy client.

        Args:
            bucket: S3 bucket name.
            proxy_port: Proxy port (default from env or 18080).
            delay: Artificial delay in seconds per operation.
            bandwidth_bps: Bandwidth limit in bytes/sec (unused).
        """
        self.bucket = bucket
        self.delay = delay
        self._port = proxy_port or int(
            os.environ.get("S3POOL_PROXY_PORT", "18080")
        )
        self._host = "127.0.0.1"
        self._conn: http.client.HTTPConnection | None = None
        _RustProxyManager.get_instance().ensure_running(self._port)

    def _get_conn(self) -> http.client.HTTPConnection:
        if self._conn is None:
            self._conn = http.client.HTTPConnection(
                self._host, self._port, timeout=300
            )
        return self._conn

    def _reset_conn(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except OSError:
                pass
            self._conn = None

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        if self.delay > 0:
            time.sleep(self.delay)
        retries = 3
        for attempt in range(retries):
            try:
                conn = self._get_conn()
                hdrs = headers or {}
                if body and "Content-Length" not in hdrs:
                    hdrs["Content-Length"] = str(len(body))
                conn.request(method, path, body=body, headers=hdrs)
                resp = conn.getresponse()
                data = resp.read()
                return resp.status, dict(resp.getheaders()), data
            except (
                http.client.HTTPException,
                ConnectionError,
                OSError,
                BrokenPipeError,
            ):
                self._reset_conn()
                if attempt < retries - 1:
                    time.sleep(0.1 * (2 ** attempt))
                else:
                    raise

    def put_object(self, key: str, data: bytes) -> str:
        """Upload an object via the Rust proxy."""
        path = f"/{self.bucket}/{key}"
        status, headers, body = self._request(
            "PUT",
            path,
            body=data,
            headers={"Content-Type": "application/octet-stream"},
        )
        if status not in (200, 201):
            raise RuntimeError(
                f"PUT {key} failed: HTTP {status} - "
                f"{body[:200]}"
            )
        return headers.get("ETag", "")

    def get_object(self, key: str) -> bytes:
        """Download an object via the Rust proxy."""
        path = f"/{self.bucket}/{key}"
        status, _headers, body = self._request("GET", path)
        if status != 200:
            raise RuntimeError(
                f"GET {key} failed: HTTP {status} - "
                f"{body[:200]}"
            )
        return body

    def get_object_drain(self, path: str) -> int:
        """Download an object, discard body, return byte count.

        Uses Content-Length header (already parsed by http.client) to
        avoid per-chunk ``len()`` + accumulation. Drains in 1MB chunks
        to minimize Python ``read()`` calls for large objects.

        Args:
            path: Full URL path (e.g. ``/bucket/key``).

        Returns:
            Total bytes read.
        """
        conn = self._get_conn()
        conn.request("GET", path)
        resp = conn.getresponse()
        if resp.status != 200:
            body = resp.read()
            raise RuntimeError(
                f"GET {path} failed: HTTP {resp.status} - "
                f"{body[:200]}"
            )
        # Grab Content-Length before draining (resp.length is set by http.client)
        nbytes = resp.length or 0
        # Drain body to free connection — 1MB chunks = fewer read() calls
        while resp.read(1048576):
            pass
        return nbytes

    def delete_object(self, key: str) -> None:
        """Delete a single object via the Rust proxy."""
        path = f"/{self.bucket}/{key}"
        status, _headers, body = self._request("DELETE", path)
        if status not in (200, 204):
            raise RuntimeError(
                f"DELETE {key} failed: HTTP {status} - "
                f"{body[:200]}"
            )

    def delete_objects(self, keys: list[str]) -> dict:
        """Batch delete up to 1000 objects via the Rust proxy."""
        if not keys:
            return {"Deleted": [], "Errors": []}
        xml_parts = [
            '<?xml version="1.0" encoding="UTF-8"?><Delete>'
        ]
        for key in keys:
            escaped = (
                key.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            xml_parts.append(
                f"<Object><Key>{escaped}</Key></Object>"
            )
        xml_parts.append("</Delete>")
        xml_body = "".join(xml_parts).encode("utf-8")
        md5_b64 = b64encode(md5(xml_body).digest()).decode("ascii")
        path = f"/{self.bucket}/?delete="
        status, _headers, body = self._request(
            "POST",
            path,
            body=xml_body,
            headers={
                "Content-Type": "application/xml",
                "Content-MD5": md5_b64,
            },
        )
        if status != 200:
            raise RuntimeError(
                f"Batch DELETE failed: HTTP {status} - "
                f"{body[:200]}"
            )
        return self._parse_delete_response(body)

    def head_object(self, key: str) -> dict:
        """Get object metadata via the Rust proxy."""
        path = f"/{self.bucket}/{key}"
        status, headers, body = self._request("HEAD", path)
        if status != 200:
            raise RuntimeError(
                f"HEAD {key} failed: HTTP {status} - "
                f"{body[:200]}"
            )
        return headers

    def list_objects(
        self,
        prefix: str = "",
        max_keys: int = 1000,
        continuation_token: str | None = None,
    ) -> dict:
        """List objects (ListObjectsV2) via the Rust proxy."""
        path = (
            f"/{self.bucket}/?list-type=2&max-keys={max_keys}"
        )
        if prefix:
            path += f"&prefix={url_quote(prefix, safe='')}"
        if continuation_token:
            path += (
                f"&continuation-token="
                f"{url_quote(continuation_token, safe='')}"
            )
        status, _headers, body = self._request("GET", path)
        if status != 200:
            raise RuntimeError(
                f"LIST failed: HTTP {status} - {body[:200]}"
            )
        return self._parse_list_response(body)

    def close(self) -> None:
        """Close the HTTP connection."""
        self._reset_conn()

    @staticmethod
    def _parse_list_response(xml_data: bytes) -> dict:
        result: dict[str, Any] = {
            "Contents": [],
            "CommonPrefixes": [],
            "IsTruncated": False,
            "NextContinuationToken": None,
            "KeyCount": 0,
        }
        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError:
            return result

        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        for content in root.findall(f"{ns}Contents"):
            obj: dict[str, Any] = {}
            for tag in ("Key", "Size", "LastModified", "ETag"):
                el = content.find(f"{ns}{tag}")
                if el is not None:
                    if tag == "Size":
                        obj[tag] = int(el.text or "0")
                    else:
                        obj[tag] = el.text or ""
            result["Contents"].append(obj)

        trunc = root.find(f"{ns}IsTruncated")
        if trunc is not None:
            result["IsTruncated"] = (
                (trunc.text or "").lower() == "true"
            )

        token = root.find(f"{ns}NextContinuationToken")
        if token is not None:
            result["NextContinuationToken"] = token.text

        count = root.find(f"{ns}KeyCount")
        if count is not None:
            result["KeyCount"] = int(count.text or "0")

        return result

    @staticmethod
    def _parse_delete_response(xml_data: bytes) -> dict:
        result: dict[str, list] = {"Deleted": [], "Errors": []}
        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError:
            return result

        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        for deleted in root.findall(f"{ns}Deleted"):
            key_el = deleted.find(f"{ns}Key")
            if key_el is not None:
                result["Deleted"].append(
                    {"Key": key_el.text or ""}
                )

        for error in root.findall(f"{ns}Error"):
            err: dict[str, str] = {}
            for tag in ("Key", "Code", "Message"):
                el = error.find(f"{ns}{tag}")
                if el is not None:
                    err[tag] = el.text or ""
            result["Errors"].append(err)

        return result
