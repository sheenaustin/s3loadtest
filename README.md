# s3loadtest

A modular S3 load testing framework that generates realistic, production-like workloads against any S3-compatible storage. Seven distinct test patterns simulate everything from steady-state CRUD to bursty checkpoint saves to slow WAN clients — run them individually or all at once across a fleet of workers.

## Why s3loadtest?

Most S3 benchmarks measure peak throughput under ideal conditions. Real storage clusters face mixed workloads: continuous background I/O, periodic checkpoint bursts, slow clients holding connections open, and aggressive batch deletes. s3loadtest reproduces these patterns so you can test how your storage actually behaves under realistic pressure.

## Quick Start

### 1. Install

```bash
pip install .
```

Or install directly from the repo:

```bash
pip install git+https://github.com/sheenaustin/s3loadtest.git
```

### 2. Configure

Copy the example config and fill in your S3 credentials:

```bash
cp .env.example .env
```

Edit `.env` with your values:

```ini
S3_ENDPOINTS=https://your-s3-endpoint:9000
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET=your-bucket-name
```

### 3. Initialize test data

Pre-generate uncompressible data files (1KB to 100MB) used by the tests:

```bash
s3loadtest init-data
```

### 4. Run a test

```bash
# Run baseload locally for 5 minutes
s3loadtest run baseload --duration 5m

# Run with debug logging
s3loadtest run baseload --duration 5m --log-level DEBUG
```

That's it. You're load testing.

## Test Patterns

| Test | Behavior | Read/Write | Schedule |
|------|----------|-----------|----------|
| **baseload** | Continuous mixed I/O | 70% read, 30% write | 24/7 |
| **checkpoint** | Large write bursts | 10% read, 90% write | 10m burst, 1h gap |
| **heavyread** | Read-only storm | 100% read | 20m burst, 40m gap |
| **spiky** | Baseload with random extreme bursts | Variable | 1-3 bursts per 5m window |
| **elephant** | Simulated slow WAN clients (1 Mbps) | 50/50 | 24/7 |
| **delete** | Streaming batch delete | Delete only | Every 3m |
| **listops** | Continuous paginated listing | List only | 24/7 |

Run all tests simultaneously:

```bash
s3loadtest run all --duration 1h
```

## Distributed Mode

Scale across multiple machines using SSH-based worker orchestration.

### Setup workers

Create a `workers` file with one hostname per line:

```
worker-1
worker-2
worker-3
```

Make sure s3loadtest is installed on each worker and SSH access is configured.

### Launch across the fleet

```bash
# Start baseload on all workers for 1 hour
s3loadtest start baseload --duration 1h

# Start all tests on 10 workers
s3loadtest start all --duration 2h --workers 10

# Check what's running
s3loadtest status

# Stop everything
s3loadtest stop all
```

### Cleanup

Remove all test objects from S3 when you're done:

```bash
s3loadtest cleanup
s3loadtest cleanup --prefix loadtest/
```

## S3 Backends

s3loadtest supports three S3 client backends. Set `S3LOADTEST_BACKEND` in your `.env`:

| Backend | Value | Description |
|---------|-------|-------------|
| **boto3** | `boto3` (default) | Standard AWS SDK. Works with any S3-compatible storage. |
| **s3pool** | `proxy` | Routes through [s3pool](https://github.com/sheenaustin/s3pool) proxy for endpoint rotation and connection pooling. |
| **minio** | `minio` | Uses the MinIO Python SDK. |

## Configuration Reference

All configuration is via environment variables or a `.env` file. See [.env.example](.env.example) for the full list.

**Required:**
- `S3_ENDPOINTS` — S3 endpoint URLs (comma-separated)
- `AWS_ACCESS_KEY_ID` — Access key
- `AWS_SECRET_ACCESS_KEY` — Secret key
- `S3_BUCKET` — Bucket name

**Optional:**
- `S3LOADTEST_BACKEND` — Client backend: `boto3`, `proxy`, `minio` (default: `boto3`)
- `S3LOADTEST_DATA_DIR` — Pre-generated data directory (default: `./data`)
- `S3LOADTEST_LOG_DIR` — Log output directory (default: `./logs`)
- `S3LOADTEST_WORKERS_FILE` — Workers file path (default: `./workers`)
- `S3_VERIFY_SSL` — SSL verification (default: `false`)

## CLI Reference

```
s3loadtest init-data                          # Generate test data files
s3loadtest run <test> --duration <dur>        # Run a test locally
s3loadtest start <test> --duration <dur>      # Launch on workers via SSH
s3loadtest status                             # Show running tests
s3loadtest stop <test|all>                    # Stop tests on workers
s3loadtest cleanup [--prefix <prefix>]        # Delete test objects from S3
```

Duration format: `5m` (minutes), `1h` (hours), `2d` (days), `1w` (weeks).

## Architecture

```
s3loadtest/
  __main__.py       CLI entry point
  config.py         Environment-based configuration
  s3_client.py      S3 client factory (backend selection)
  backends.py       S3 client implementations (boto3, s3pool, minio)
  s3_ops.py         Retry-wrapped S3 operations
  workers.py        SSH-based worker management
  proxy.py          s3pool proxy management on workers
  utils.py          Retry logic, data generation, formatting
  keyfiles.py       Thread-safe object key tracking (fcntl locking)
  logging_setup.py  Structured logging with JSON support
  tests/
    base.py         Base class with stats, signals, threading
    baseload.py     Continuous 70/30 read/write
    checkpoint.py   Bursty large writes
    heavyread.py    Read-only storms
    spiky.py        Baseload + random extreme bursts
    elephant.py     Simulated slow WAN clients
    delete.py       Streaming batch delete
    listops.py      Continuous paginated listing
  cli/
    run.py          Local test execution
    start.py        Distributed test launch
    stop.py         Process termination
    status.py       Running test discovery
    cleanup.py      S3 object cleanup
    init_data.py    Test data initialization
```

## Requirements

- Python 3.9+
- `boto3` (only external dependency)
- SSH access to workers (for distributed mode)

## License

MIT
