# ExchangeDB Operations Guide

This guide covers database administration tasks: backup and restore, monitoring,
retention policies, WAL management, partition management, and performance
tuning.

---

## Table of Contents

1. [Backup and Restore](#backup-and-restore)
2. [Point-in-Time Recovery (PITR)](#point-in-time-recovery-pitr)
3. [Monitoring](#monitoring)
4. [Retention Policies and TTL](#retention-policies-and-ttl)
5. [Tiered Storage](#tiered-storage)
6. [WAL Configuration](#wal-configuration)
7. [Partition Management](#partition-management)
8. [VACUUM](#vacuum)
9. [Index Management](#index-management)
10. [Log Management](#log-management)
11. [Health Checks](#health-checks)
12. [Background Jobs](#background-jobs)
13. [Hot Configuration Reload](#hot-configuration-reload)
14. [Capacity Planning](#capacity-planning)

---

## Backup and Restore

### Snapshots

ExchangeDB supports point-in-time snapshots for backup.

**Create a snapshot via CLI:**

```bash
exchange-db snapshot --output /backup/exchangedb-$(date +%Y%m%d)/
```

**Create a snapshot via HTTP:**

```bash
curl -X POST http://localhost:9000/admin/checkpoint
```

The checkpoint operation flushes all pending WAL segments to the column store,
ensuring a consistent on-disk state.

### Restore from Snapshot

```bash
# Stop the server
systemctl stop exchangedb

# Restore from backup
exchange-db restore --input /backup/exchangedb-20240301/

# Start the server
systemctl start exchangedb
```

### File-Level Backup

Since ExchangeDB stores data as regular files in the data directory, you can
use standard file system tools for backup:

```bash
# Create a consistent snapshot (checkpoint first)
curl -X POST http://localhost:9000/admin/checkpoint

# File-level copy
rsync -av /data/exchangedb/ /backup/exchangedb-$(date +%Y%m%d)/

# Or with compression
tar czf /backup/exchangedb-$(date +%Y%m%d).tar.gz /data/exchangedb/
```

### Partition-Level Backup

For very large databases, back up individual partitions:

```bash
# Detach old partitions
psql -h localhost -p 8812 -c \
  "ALTER TABLE trades DETACH PARTITION '2024-01-01';"

# Copy detached partition
cp -r /data/exchangedb/trades/2024-01-01.detached /backup/partitions/

# Re-attach
psql -h localhost -p 8812 -c \
  "ALTER TABLE trades ATTACH PARTITION '2024-01-01';"
```

### Automated Backup Script

```bash
#!/bin/bash
# /etc/cron.daily/exchangedb-backup

BACKUP_DIR="/backup/exchangedb"
DATA_DIR="/data/exchangedb"
RETENTION_DAYS=7

# Checkpoint
curl -s -X POST http://localhost:9000/admin/checkpoint

# Create timestamped backup
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p "${BACKUP_DIR}/${TIMESTAMP}"
rsync -a "${DATA_DIR}/" "${BACKUP_DIR}/${TIMESTAMP}/"

# Cleanup old backups
find "${BACKUP_DIR}" -maxdepth 1 -type d -mtime +${RETENTION_DAYS} \
  -exec rm -rf {} \;

echo "Backup completed: ${BACKUP_DIR}/${TIMESTAMP}"
```

---

## Point-in-Time Recovery (PITR)

ExchangeDB supports PITR through WAL-based checkpoints. PITR allows you to
restore the database to any point in time within the WAL retention window.

### How PITR Works

1. Background PITR checkpoint jobs run periodically, recording the WAL
   position and a consistent snapshot marker.
2. WAL segments are retained until they are no longer needed for PITR.
3. To recover, you restore a base snapshot and replay WAL segments up to the
   desired point in time.

### Configuration

PITR checkpoints are registered as a background job at server startup. The
checkpoint interval is configurable:

```toml
[storage]
wal_enabled = true
```

### Recovery Procedure

1. Stop the server.
2. Restore the most recent base snapshot.
3. Copy WAL segments from the backup into the WAL directory.
4. Start the server -- it will replay WAL segments during startup recovery.

---

## Monitoring

### Prometheus Metrics

ExchangeDB exposes Prometheus-compatible metrics at `GET /metrics`:

```bash
curl http://localhost:9000/metrics
```

#### Available Metrics

**Query metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `exchangedb_queries_total` | counter | Total queries executed |
| `exchangedb_queries_failed_total` | counter | Total failed queries |
| `exchangedb_query_duration_seconds` | histogram | Query execution time distribution |

**Write metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `exchangedb_rows_written_total` | counter | Total rows written |
| `exchangedb_rows_read_total` | counter | Total rows read |
| `exchangedb_ilp_lines_total` | counter | Total ILP lines ingested |

**Connection metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `exchangedb_active_connections` | gauge | Current active HTTP connections |
| `exchangedb_tables_count` | gauge | Number of tables |

#### Prometheus Scrape Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'exchangedb'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:9000']
    metrics_path: '/metrics'
```

#### Grafana Dashboard

Create a Grafana dashboard with these panels:

1. **Query Rate**: `rate(exchangedb_queries_total[5m])`
2. **Query Latency p99**: `histogram_quantile(0.99, rate(exchangedb_query_duration_seconds_bucket[5m]))`
3. **Write Rate**: `rate(exchangedb_rows_written_total[5m])`
4. **ILP Ingestion Rate**: `rate(exchangedb_ilp_lines_total[5m])`
5. **Error Rate**: `rate(exchangedb_queries_failed_total[5m])`
6. **Active Connections**: `exchangedb_active_connections`
7. **Table Count**: `exchangedb_tables_count`

### Diagnostics Endpoint

```bash
curl http://localhost:9000/api/v1/diagnostics | jq
```

Returns comprehensive system information:

```json
{
  "version": "0.1.0",
  "rust_version": "1.85",
  "os": "linux",
  "arch": "x86_64",
  "pid": 12345,
  "uptime_secs": 3600,
  "memory": { "rss_bytes": 104857600, "heap_bytes": 50000000 },
  "storage": {
    "data_dir": "/data",
    "disk_free_bytes": 536870912000,
    "tables": 5,
    "total_rows": 15000000
  },
  "connections": { "http": 10, "pgwire": 3, "ilp": 2 },
  "wal": { "pending_segments": 0, "applied_segments": 42 },
  "config": { "http_port": 9000, "pg_port": 8812, "ilp_port": 9009 }
}
```

### Slow Query Log

ExchangeDB logs queries that exceed a configurable threshold:

```bash
curl http://localhost:9000/admin/slow-queries
```

Returns:

```json
{
  "queries": [
    {
      "sql": "SELECT * FROM trades WHERE price > 60000",
      "duration_ms": 1250.5,
      "rows": 500000
    }
  ]
}
```

### EXPLAIN ANALYZE

Use `EXPLAIN ANALYZE` to profile individual queries:

```sql
EXPLAIN ANALYZE SELECT symbol, avg(price)
FROM trades
WHERE timestamp > '2024-01-01'
GROUP BY symbol;
```

This executes the query and returns per-stage timing information.

---

## Retention Policies and TTL

### Automatic Retention

Configure automatic data expiration in `exchange-db.toml`:

```toml
[retention]
enabled = true
max_age = "30d"           # Delete data older than 30 days
check_interval = "1h"     # Check every hour
```

The retention background job runs at `check_interval` and drops partitions
older than `max_age`.

### Manual Retention

Drop old partitions manually:

```sql
-- Delete all data older than 90 days
DELETE FROM trades WHERE timestamp < dateadd('d', -90, now());

-- Or detach old partitions (faster, reversible)
ALTER TABLE trades DETACH PARTITION '2024-01-01';
ALTER TABLE trades DETACH PARTITION '2024-01-02';
```

### Per-Table TTL

TTL enforcement is registered as a background job. Individual tables can have
different retention policies through partition management:

```sql
-- Archive old data to a different table before dropping
INSERT INTO trades_archive
SELECT * FROM trades WHERE timestamp < dateadd('d', -90, now());

DELETE FROM trades WHERE timestamp < dateadd('d', -90, now());
```

---

## Tiered Storage

ExchangeDB supports tiered storage for cost-effective data management:

| Tier | Storage | Purpose |
|------|---------|---------|
| Hot | Local SSD | Recent data, fast queries |
| Warm | Local HDD / Network | Older data, moderate query speed |
| Cold | Object storage (S3) | Archive, rarely queried |

### Configuration

Tiered storage is configured via `exchange-db.toml` and is managed by
background jobs that move partitions between tiers based on age:

```toml
[storage]
# Configure tier paths in the data directory structure
# Hot tier: default data_dir (SSD)
# Warm/cold: configured via tiered storage settings
```

### How It Works

1. New data is written to the **hot** tier (local SSD).
2. A background job periodically checks partition ages.
3. Partitions older than the hot threshold are moved to **warm** storage.
4. Partitions older than the warm threshold are moved to **cold** storage.
5. Queries transparently read from all tiers.

### Moving Partitions Manually

```sql
-- Detach from hot storage
ALTER TABLE trades DETACH PARTITION '2024-01-01';

-- Move files to warm storage
-- (done at the file system level)

-- Re-attach from warm storage
ALTER TABLE trades ATTACH PARTITION '2024-01-01';
```

---

## WAL Configuration

The Write-Ahead Log (WAL) ensures data durability and enables replication.

### Configuration Options

```toml
[storage]
wal_enabled = true                # Enable/disable WAL
wal_max_segment_size = "64MB"     # Max size before rotation
```

```toml
[performance]
writer_commit_mode = "async"      # "sync" or "async"
```

| Option | Description | Default |
|--------|-------------|---------|
| `wal_enabled` | Enable WAL for crash recovery | `true` |
| `wal_max_segment_size` | Maximum WAL segment size | `64MB` |
| `writer_commit_mode` | `sync` = fsync each commit, `async` = batch fsync | `async` |

### WAL Status

Check WAL status via the admin API:

```bash
curl http://localhost:9000/admin/wal
```

```json
{
  "wal_enabled": true,
  "total_segments": 15,
  "total_bytes": 67108864
}
```

### WAL Merge

The WAL merge background job replays WAL events into the column store. It runs
continuously and merges segments as they become available.

### Checkpoint

Force a WAL checkpoint (flush all pending segments):

```bash
curl -X POST http://localhost:9000/admin/checkpoint
```

This is recommended before:

- Taking a backup.
- Detaching partitions.
- Shutting down the server.

---

## Partition Management

### Viewing Partitions

```bash
curl http://localhost:9000/admin/partitions/trades
```

```json
{
  "table": "trades",
  "partitions": [
    {"name": "2024-03-14", "row_count": 50000, "size_bytes": 4194304},
    {"name": "2024-03-15", "row_count": 75000, "size_bytes": 6291456}
  ]
}
```

### Detach Partition

Detaching a partition makes it unavailable for queries but preserves it on disk:

```sql
ALTER TABLE trades DETACH PARTITION '2024-01-01';
```

The partition directory is renamed from `2024-01-01/` to `2024-01-01.detached/`.

### Attach Partition

Re-attach a previously detached partition:

```sql
ALTER TABLE trades ATTACH PARTITION '2024-01-01';
```

### Squash Partitions

Merge two adjacent partitions into one:

```sql
ALTER TABLE trades SQUASH PARTITIONS '2024-01-01' '2024-01-02';
```

This is useful for consolidating many small partitions into fewer larger ones.

### Partition Pruning

The query optimizer automatically prunes partitions based on WHERE clause
timestamp filters:

```sql
-- Only scans partitions from March 2024
SELECT * FROM trades WHERE timestamp BETWEEN '2024-03-01' AND '2024-03-31';
```

Partition pruning provides up to 55x speedup compared to full table scans.

---

## VACUUM

VACUUM reclaims disk space from:

- Applied WAL segments that have been merged into the column store.
- Empty partitions after DELETE operations.
- Deleted column files after ALTER TABLE DROP COLUMN.

### SQL

```sql
VACUUM;              -- All tables
VACUUM trades;       -- Specific table
```

### HTTP

```bash
curl -X POST http://localhost:9000/admin/vacuum/trades
```

### When to VACUUM

- After large DELETE operations.
- After DROP COLUMN.
- Periodically if WAL segments accumulate.
- When disk space is running low.

### Automated VACUUM

Set up a cron job:

```bash
# Every day at 3 AM
0 3 * * * curl -s -X POST http://localhost:9000/admin/vacuum/trades
```

---

## Index Management

ExchangeDB automatically manages indexes:

- **Bitmap indexes** are created automatically on all SYMBOL columns.
- **Symbol maps** (string-to-integer dictionaries) are maintained automatically.
- No manual `CREATE INDEX` or `DROP INDEX` is needed.

### How Bitmap Indexes Work

Each SYMBOL column has two index files:

- `.k` (key file): Maps symbol IDs to row ID ranges.
- `.v` (value file): Sorted lists of row IDs for each symbol.

When a query filters on a SYMBOL column (`WHERE symbol = 'BTC/USD'`), the
executor looks up the symbol's integer ID and uses the bitmap index to find
matching rows directly, skipping the full column scan.

### Index Performance

Bitmap index lookups achieve 767.50M lookups/s in benchmarks.

---

## Log Management

### Log Levels

Set the log level via configuration:

```toml
[server]
log_level = "info"    # trace, debug, info, warn, error
```

Or via environment variable:

```bash
EXCHANGEDB_LOG_LEVEL=debug exchange-db server
```

Or change at runtime:

```bash
curl -X POST http://localhost:9000/admin/config \
  -H 'Content-Type: application/json' \
  -d '{"log_level": "debug"}'
```

### Log Rotation

ExchangeDB includes built-in log rotation:

- Logs are written to `<prefix>.log` in the configured directory.
- Files are rotated when they exceed `max_size` bytes.
- At most `max_files` rotated files are kept (`.1`, `.2`, ...).
- Thread-safe via internal mutex.

### Structured Logging

ExchangeDB uses `tracing` for structured logging. Output formats:

- **Text** (default): Human-readable format for development.
- **JSON**: Machine-readable format for log aggregation (ELK, Datadog, etc.).

### Audit Logging

When security features are enabled, ExchangeDB writes an audit log in NDJSON
format with daily rotation. Audit events include:

- Authentication attempts (success/failure).
- Authorization decisions (granted/denied).
- DDL operations (CREATE, ALTER, DROP).
- Data access patterns.

---

## Health Checks

### HTTP Health Check

```bash
curl http://localhost:9000/health
# or
curl http://localhost:9000/api/v1/health
```

Returns `200 OK` with:

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_secs": 3600.5
}
```

### Docker Health Check

The provided `docker-compose.yml` includes a health check:

```yaml
healthcheck:
  test: ["CMD-SHELL", "curl -sf http://localhost:9000/health || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 3
  start_period: 5s
```

### Kubernetes Probes

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 9000
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /api/v1/health
    port: 9000
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Internal Health Checks

The health check system performs real checks:

- **Disk**: Verifies the data directory is writable and has free space.
- **Memory**: Checks RSS against configured limits.
- **WAL**: Verifies WAL is functioning and not stuck.

---

## Background Jobs

ExchangeDB runs several background jobs:

```bash
curl http://localhost:9000/admin/jobs
```

```json
{
  "jobs": [
    {"name": "wal_merge", "status": "running", "last_run": "2024-03-21T14:00:00Z"},
    {"name": "retention_check", "status": "idle", "last_run": "2024-03-21T13:00:00Z"},
    {"name": "ttl_enforcement", "status": "idle", "last_run": "2024-03-21T13:00:00Z"},
    {"name": "pitr_checkpoint", "status": "idle", "last_run": "2024-03-21T12:00:00Z"},
    {"name": "tiered_storage", "status": "idle", "last_run": null}
  ]
}
```

| Job | Description | Trigger |
|-----|-------------|---------|
| `wal_merge` | Merges WAL segments into column store | Continuous |
| `retention_check` | Drops expired partitions | Interval (`check_interval`) |
| `ttl_enforcement` | Enforces per-table TTL | Interval |
| `pitr_checkpoint` | Creates PITR checkpoints | Interval |
| `tiered_storage` | Moves partitions between tiers | Interval |

---

## Hot Configuration Reload

On Unix systems, send `SIGHUP` to reload runtime-safe settings without
restarting the server:

```bash
kill -HUP $(pidof exchange-db)
```

Settings that can be reloaded at runtime:

- Log level.
- Retention policy parameters.

Settings that require a restart:

- Bind addresses and ports.
- Data directory.
- WAL enabled/disabled.
- Replication mode.
- TLS certificates.

---

## Capacity Planning

### Storage Estimation

Calculate expected storage per day:

```
daily_bytes = rows_per_day * bytes_per_row
bytes_per_row = sum(column_sizes) + overhead
```

Column sizes (see [Data Types](DATA_TYPES.md)):

| Type | Size |
|------|------|
| TIMESTAMP | 8 bytes |
| SYMBOL | 4 bytes + dictionary overhead |
| DOUBLE | 8 bytes |
| LONG | 8 bytes |
| VARCHAR | variable (avg length + 8 byte offset) |

Example for a trades table (timestamp + symbol + price + volume + side):

```
bytes_per_row = 8 + 4 + 8 + 8 + 4 = 32 bytes
100M rows/day = 3.2 GB/day uncompressed
With LZ4 compression: ~1-2 GB/day
```

### Memory Requirements

- **Minimum**: 256 MB (small datasets, few concurrent queries).
- **Recommended**: 4-16 GB (production workloads).
- **Large deployments**: 32-128 GB (billions of rows, many concurrent queries).

Memory-mapped files allow ExchangeDB to work with datasets larger than RAM,
but more RAM means more data stays in the OS page cache.

### CPU Requirements

- **Query parallelism**: ExchangeDB uses one thread per partition during
  parallel scans. More cores benefit multi-partition queries.
- **Recommended**: 4-16 cores for production.
- **SIMD**: ExchangeDB uses SIMD instructions (AVX2, NEON) for aggregation,
  providing up to 3.97x speedup. Modern CPUs with wide SIMD units benefit most.
