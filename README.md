# s3loadtest

Stress-test any S3-compatible object store to its breaking point. Nine test patterns target different failure modes: throughput saturation, metadata server overload, connection exhaustion, bursty checkpoint storms, and aggressive batch deletes.

By default, s3loadtest automatically detects your machine's resources and uses **everything** — 512+ concurrent threads on a 64-core box, endpoint rotation across all backends, zero rate limiting. You don't configure it to be aggressive. It already is.

## Quick Start

```bash
# Install
uv sync

# Configure (edit .env with your S3 endpoints + credentials)
cp .env.example .env

# Generate test data (1KB to 100MB, uncompressible)
uv run s3loadtest init-data

# Run a test
uv run s3loadtest run firehose --duration 5m
```

## Test Patterns

Each test destroys something different.

| Test | What it does | What it kills |
|------|-------------|---------------|
| **firehose** | 50/50 read/write, all sizes, zero sleep, max threads | Raw throughput ceiling |
| **metastorm** | Rapid 1-byte PUT/HEAD/DELETE/LIST cycles | Metadata server / IOPS |
| **elephant** | 1000+ slow-trickle connections at 100 bytes/sec | Connection pools / FD limits |
| **baseload** | 70% read, 30% write, continuous | Sustained mixed I/O |
| **checkpoint** | 90% write bursts of 10MB/100MB objects, 10m on, 1h off | Write throughput / journaling |
| **heavyread** | 100% read storm, equal size cycling, 20m bursts | Read cache / bandwidth |
| **spiky** | Baseload + random 1-minute extreme bursts | Burst handling / queueing |
| **delete** | Streaming batch delete (1000/batch, 32 workers) | Delete throughput / GC |
| **listops** | Continuous paginated listing | List/metadata scalability |

Run all of them at once:

```bash
uv run s3loadtest run all --duration 1h
```

## Usage

```bash
# Run locally
uv run s3loadtest run <test> --duration <dur>
uv run s3loadtest run firehose --duration 1h
uv run s3loadtest run metastorm --duration 30m --concurrency 200

# Override thread count (default: auto-detected, typically 512 on 64-core)
uv run s3loadtest run firehose --duration 5m --concurrency 1000

# Distributed mode (SSH workers)
uv run s3loadtest start all --duration 2h --workers 10
uv run s3loadtest status
uv run s3loadtest stop all

# Cleanup
uv run s3loadtest cleanup --prefix loadtest/

# Generate test data
uv run s3loadtest init-data
```

Duration format: `5m`, `1h`, `2d`, `1w`.

## Configuration

All via environment variables or `.env` file.

**Required:**

```ini
S3_ENDPOINTS=https://node1:9000,https://node2:9000,https://node3:9000
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET=your-bucket
```

**Optional:**

| Variable | Default | Description |
|----------|---------|-------------|
| `S3LOADTEST_BACKEND` | `proxy` | S3 client: `proxy` (s3pool), `boto3`, `minio` |
| `S3LOADTEST_DATA_DIR` | `./data` | Pre-generated data files |
| `S3LOADTEST_KEY_DIR` | `/tmp` | Object key tracking files |
| `S3LOADTEST_WORKERS_FILE` | `./workers` | SSH worker hostnames (one per line) |
| `S3_VERIFY_SSL` | `false` | SSL certificate verification |

**Distributed mode:**

| Variable | Description |
|----------|-------------|
| `S3LOADTEST_REMOTE_CODE_DIR` | Path to s3loadtest on workers |
| `S3LOADTEST_REMOTE_LOG_DIR` | Log directory on workers |
| `S3POOL_BINARY` | Path to s3pool binary (for proxy backend) |

## S3 Backends

| Backend | Set `S3LOADTEST_BACKEND` to | Description |
|---------|----------------------------|-------------|
| **s3pool** (default) | `proxy` | Routes through [s3pool](https://github.com/sheenaustin/s3pool) Rust proxy — auto-downloaded if not found |
| **boto3** | `boto3` | AWS SDK with automatic endpoint rotation across all backends |
| **minio** | `minio` | MinIO Python SDK |

The default `proxy` backend uses s3pool for TLS, SigV4 signing, and connection pooling. If the s3pool binary isn't found locally, it's downloaded automatically from GitHub releases.

## Stats Output

s3loadtest reports periodic stats with latency percentiles:

```
STATS: ops=42,691 (1,423/s), bytes=2.1GB (71.2MB/s), errors=0, elapsed=30s | GET p50=3ms p95=18ms p99=42ms | PUT p50=8ms p95=35ms p99=89ms
```

Final summary includes full latency breakdown:

```
FINAL: ops=284,512 (1,410/s), bytes=14.2GB, errors=3, elapsed=3m
  Latency: GET p50=3ms p95=19ms p99=45ms | PUT p50=9ms p95=37ms p99=91ms
```

## Object Key Layout

```
loadtest/{worker_id}/{size_label}/{test_name}/{timestamp}/{suffix}
```

Keys are tracked in per-size files for efficient size-targeted reads by heavyread.

## Architecture

```
s3loadtest/
  __main__.py       CLI entry point
  config.py         All tunables (maximized by default)
  s3_client.py      S3 client factory
  backends.py       boto3 (endpoint rotation), s3pool proxy, minio
  s3_ops.py         Retry-wrapped S3 operations
  keyfiles.py       Thread-safe per-size key tracking (fcntl)
  utils.py          Retry logic, data generation, thread calculation
  logging_setup.py  Structured logging with latency percentiles
  workers.py        SSH worker orchestration
  proxy.py          s3pool proxy lifecycle management
  tests/
    base.py         Base class: stats, latency tracking, signals
    firehose.py     Max throughput mixed workload
    metastorm.py    Metadata bombardment
    elephant.py     Connection exhaustion (slow trickle)
    baseload.py     Continuous 70/30 mixed I/O
    checkpoint.py   Bursty large writes
    heavyread.py    Read storms with equal size cycling
    spiky.py        Baseload + random extreme bursts
    delete.py       Streaming batch delete (1000/batch, 32 workers)
    listops.py      Continuous paginated listing
  cli/
    run.py          Local execution with latency reporting
    start.py        Distributed launch
    stop.py         Process termination
    status.py       Running test discovery
    cleanup.py      S3 object cleanup
    init_data.py    Test data generation
```

## Requirements

- Python 3.9+
- `boto3`
- SSH access to workers (distributed mode)
