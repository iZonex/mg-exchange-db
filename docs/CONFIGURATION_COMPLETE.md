# ExchangeDB Configuration Reference

Complete reference for every configuration option. ExchangeDB loads
configuration in priority order (highest wins):

1. CLI flags (`--bind`, `--data-dir`)
2. Environment variables (`EXCHANGEDB_*`)
3. Config file (`exchange-db.toml`)
4. Built-in defaults

---

## Table of Contents

1. [Server](#server)
2. [HTTP](#http)
3. [PostgreSQL Wire Protocol (pgwire)](#postgresql-wire-protocol-pgwire)
4. [InfluxDB Line Protocol (ILP)](#influxdb-line-protocol-ilp)
5. [Storage](#storage)
6. [Retention](#retention)
7. [Performance](#performance)
8. [Security](#security)
9. [Replication](#replication)
10. [CLI Flags](#cli-flags)
11. [Size and Duration Formats](#size-and-duration-formats)
12. [Example Configuration Files](#example-configuration-files)

---

## Server

General server settings.

| TOML Key | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `server.data_dir` | `EXCHANGEDB_DATA_DIR` | `"./data"` | Root directory for all table data, WAL, metadata, and indexes. Must be writable. |
| `server.log_level` | `EXCHANGEDB_LOG_LEVEL` | `"info"` | Logging verbosity. Values: `trace`, `debug`, `info`, `warn`, `error`. Can be changed at runtime via SIGHUP or admin API. |

### Example

```toml
[server]
data_dir = "/var/lib/exchangedb"
log_level = "info"
```

```bash
export EXCHANGEDB_DATA_DIR=/var/lib/exchangedb
export EXCHANGEDB_LOG_LEVEL=info
```

### Notes

- `data_dir` is created automatically on first startup if it does not exist.
- Changing `data_dir` requires stopping the server and moving data files.
- `log_level` can be changed at runtime via `kill -HUP` or the admin API.

---

## HTTP

HTTP REST API and web console settings.

| TOML Key | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `http.bind` | `EXCHANGEDB_HTTP_BIND` | `"0.0.0.0:9000"` | Bind address and port for the HTTP server. Serves REST API, web console, metrics, and WebSocket. |
| `http.enabled` | `EXCHANGEDB_HTTP_ENABLED` | `true` | Enable or disable the HTTP server entirely. When disabled, no HTTP, web console, or metrics endpoint is available. |

### Example

```toml
[http]
bind = "0.0.0.0:9000"
enabled = true
```

```bash
export EXCHANGEDB_HTTP_BIND=0.0.0.0:9000
export EXCHANGEDB_HTTP_ENABLED=true
```

### Notes

- The web console is served at `GET /` on the HTTP port.
- Prometheus metrics are at `GET /metrics`.
- WebSocket is at `GET /api/v1/ws`.
- CORS is enabled for all origins by default.
- When TLS is configured, HTTP becomes HTTPS.

---

## PostgreSQL Wire Protocol (pgwire)

PostgreSQL wire protocol settings for psql, DBeaver, Grafana, and other
PostgreSQL-compatible clients.

| TOML Key | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `pgwire.bind` | `EXCHANGEDB_PGWIRE_BIND` | `"0.0.0.0:8812"` | Bind address and port for the PostgreSQL wire protocol server. |
| `pgwire.enabled` | `EXCHANGEDB_PGWIRE_ENABLED` | `true` | Enable or disable the pgwire server. |

### Example

```toml
[pgwire]
bind = "0.0.0.0:8812"
enabled = true
```

```bash
export EXCHANGEDB_PGWIRE_BIND=0.0.0.0:8812
export EXCHANGEDB_PGWIRE_ENABLED=true
```

### Notes

- The default port 8812 matches QuestDB's pgwire port.
- Supports both Simple Query Protocol and Extended Query Protocol
  (prepared statements, parameterized queries).
- Supports COPY protocol for bulk data loading.
- When TLS is configured, pgwire supports SSL/TLS connections.

---

## InfluxDB Line Protocol (ILP)

ILP ingestion settings for high-throughput data ingestion.

| TOML Key | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `ilp.bind` | `EXCHANGEDB_ILP_BIND` | `"0.0.0.0:9009"` | Bind address and port for the ILP TCP/UDP server. |
| `ilp.enabled` | `EXCHANGEDB_ILP_ENABLED` | `true` | Enable or disable the ILP server. |
| `ilp.batch_size` | `EXCHANGEDB_ILP_BATCH_SIZE` | `1000` | Number of ILP lines to batch before flushing to the WAL. Higher values increase throughput but add latency. |

### Example

```toml
[ilp]
bind = "0.0.0.0:9009"
enabled = true
batch_size = 1000
```

```bash
export EXCHANGEDB_ILP_BIND=0.0.0.0:9009
export EXCHANGEDB_ILP_ENABLED=true
export EXCHANGEDB_ILP_BATCH_SIZE=1000
```

### Notes

- Port 9009 matches QuestDB's ILP port.
- The same port is used for both TCP and UDP.
- Tables are auto-created on first ILP write if they do not exist.
- Batch size trade-off: larger batches = higher throughput but higher tail
  latency for individual writes.
- On replicas (`replication.mode = "replica"`), ILP should be disabled.

---

## Storage

Storage engine and WAL settings.

| TOML Key | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `storage.wal_enabled` | `EXCHANGEDB_WAL_ENABLED` | `true` | Enable Write-Ahead Log for crash recovery and replication. Disabling WAL improves write throughput but risks data loss on crash. |
| `storage.wal_max_segment_size` | `EXCHANGEDB_WAL_MAX_SEGMENT_SIZE` | `"64MB"` | Maximum size of a single WAL segment file before rotation. Larger segments reduce rotation frequency but increase memory usage. |
| `storage.default_partition_by` | `EXCHANGEDB_DEFAULT_PARTITION_BY` | `"day"` | Default partitioning strategy for auto-created tables (via ILP). Values: `none`, `hour`, `day`, `week`, `month`, `year`. |
| `storage.mmap_page_size` | `EXCHANGEDB_MMAP_PAGE_SIZE` | `"4KB"` | Memory map page size. Use `"2MB"` for huge pages on Linux (requires OS-level huge page configuration). |

### Example

```toml
[storage]
wal_enabled = true
wal_max_segment_size = "64MB"
default_partition_by = "day"
mmap_page_size = "4KB"
```

```bash
export EXCHANGEDB_WAL_ENABLED=true
export EXCHANGEDB_WAL_MAX_SEGMENT_SIZE=64MB
export EXCHANGEDB_DEFAULT_PARTITION_BY=day
export EXCHANGEDB_MMAP_PAGE_SIZE=4KB
```

### Notes

- WAL is required for:
  - Crash recovery.
  - UPDATE and DELETE operations.
  - Replication.
  - Point-in-time recovery.
- Disabling WAL means data is written directly to the column store. This is
  faster but means any data in memory at the time of a crash is lost.
- `mmap_page_size = "2MB"` requires Linux huge pages:
  ```bash
  echo 1024 | sudo tee /proc/sys/vm/nr_hugepages
  ```

---

## Retention

Automatic data retention and TTL settings.

| TOML Key | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `retention.enabled` | `EXCHANGEDB_RETENTION_ENABLED` | `false` | Enable automatic data retention. When enabled, partitions older than `max_age` are dropped. |
| `retention.max_age` | `EXCHANGEDB_RETENTION_MAX_AGE` | `"30d"` | Maximum age of data. Partitions with all data older than this are dropped. Duration format. |
| `retention.check_interval` | `EXCHANGEDB_RETENTION_CHECK_INTERVAL` | `"1h"` | How often the retention background job checks for expired partitions. Duration format. |

### Example

```toml
[retention]
enabled = true
max_age = "90d"
check_interval = "6h"
```

```bash
export EXCHANGEDB_RETENTION_ENABLED=true
export EXCHANGEDB_RETENTION_MAX_AGE=90d
export EXCHANGEDB_RETENTION_CHECK_INTERVAL=6h
```

### Notes

- Retention drops entire partitions, not individual rows. A partition is
  dropped only when ALL rows in it are older than `max_age`.
- For finer-grained retention, use `DELETE FROM table WHERE timestamp < ...`.
- Retention applies to ALL tables. Per-table retention requires manual
  partition management.

---

## Performance

Query execution and write performance settings.

| TOML Key | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `performance.query_parallelism` | `EXCHANGEDB_QUERY_PARALLELISM` | `0` | Number of threads for parallel query execution. `0` means auto-detect (uses number of CPU cores). |
| `performance.writer_commit_mode` | `EXCHANGEDB_WRITER_COMMIT_MODE` | `"async"` | WAL commit mode. `"sync"` calls fsync after each commit (durable). `"async"` batches fsync calls (faster, small durability window). |

### Example

```toml
[performance]
query_parallelism = 8
writer_commit_mode = "async"
```

```bash
export EXCHANGEDB_QUERY_PARALLELISM=8
export EXCHANGEDB_WRITER_COMMIT_MODE=async
```

### Notes

- `query_parallelism = 0` is recommended for most deployments.
- Setting parallelism higher than CPU cores provides no benefit.
- `writer_commit_mode = "sync"` ensures every write is durable to disk
  before acknowledgment, at the cost of write throughput (~990K rows/s vs
  ~8M rows/s for async).
- For financial trading data where every trade must be durable, use `"sync"`.
- For analytics/metrics where minor data loss on crash is acceptable, use
  `"async"`.

---

## Security

Authentication, authorization, TLS, and encryption settings.

| TOML Key | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `security.auth_enabled` | `EXCHANGEDB_AUTH_ENABLED` | `false` | Enable authentication. When false, all endpoints are accessible without credentials. |
| `security.token` | `EXCHANGEDB_TOKEN` | `""` | Static bearer token for simple authentication. Used when `auth_enabled = true` and no OAuth is configured. |
| `security.oauth_issuer` | `EXCHANGEDB_OAUTH_ISSUER` | `""` | OAuth 2.0 / OIDC issuer URL for JWT validation. When set, JWTs are validated against this issuer's JWKS. |
| `security.tls_cert` | `EXCHANGEDB_TLS_CERT` | `""` | Path to TLS certificate file (PEM format). When set with `tls_key`, HTTP becomes HTTPS and pgwire supports SSL. |
| `security.tls_key` | `EXCHANGEDB_TLS_KEY` | `""` | Path to TLS private key file (PEM format). |

### Example

```toml
[security]
auth_enabled = true
token = "exdb-prod-a1b2c3d4e5f6"
# oauth_issuer = "https://auth.example.com/realms/exchangedb"
tls_cert = "/etc/exchangedb/server.crt"
tls_key = "/etc/exchangedb/server.key"
```

```bash
export EXCHANGEDB_AUTH_ENABLED=true
export EXCHANGEDB_TOKEN=exdb-prod-a1b2c3d4e5f6
export EXCHANGEDB_TLS_CERT=/etc/exchangedb/server.crt
export EXCHANGEDB_TLS_KEY=/etc/exchangedb/server.key
```

### Notes

- Token authentication and OAuth can be used simultaneously.
- OAuth tokens (JWTs) take precedence over the static token.
- TLS certificates are loaded at startup. Changing certificates requires
  a server restart.
- Auth-related endpoints (`/auth/*`) are always accessible without credentials.
- Admin endpoints (`/admin/*`) require the `Admin` permission when RBAC is
  enabled.

---

## Replication

Primary-replica replication settings.

| TOML Key | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `replication.mode` | `EXCHANGEDB_REPLICATION_MODE` | `"standalone"` | Replication mode. `"standalone"` = no replication. `"primary"` = accepts writes, ships WAL. `"replica"` = read-only, receives WAL. |
| `replication.primary_addr` | `EXCHANGEDB_PRIMARY_ADDR` | `""` | Address of the primary server. Required when `mode = "replica"`. Format: `host:port`. |

### Example: Primary

```toml
[replication]
mode = "primary"
```

### Example: Replica

```toml
[replication]
mode = "replica"
primary_addr = "primary.internal:9009"
```

```bash
export EXCHANGEDB_REPLICATION_MODE=replica
export EXCHANGEDB_PRIMARY_ADDR=primary.internal:9009
```

### Notes

- Changing replication mode requires a server restart.
- WAL must be enabled (`storage.wal_enabled = true`) for replication to work.
- Replicas reject all write operations (INSERT, UPDATE, DELETE, DDL).
- See [Replication Guide](REPLICATION.md) for setup instructions.

---

## CLI Flags

CLI flags override all other configuration sources.

| Flag | Description | Example |
|------|-------------|---------|
| `--config <path>` | Path to config file | `--config /etc/exchangedb/exchange-db.toml` |
| `--bind <addr>` | HTTP bind address | `--bind 127.0.0.1:9000` |
| `--data-dir <path>` | Data directory | `--data-dir /var/lib/exchangedb` |

### Examples

```bash
# All defaults
exchange-db server

# Custom config file
exchange-db server --config /etc/exchangedb/exchange-db.toml

# Override bind and data dir
exchange-db server --bind 0.0.0.0:9000 --data-dir /data/exchangedb

# CLI query
exchange-db sql "SELECT * FROM trades LIMIT 10"

# Import CSV
exchange-db import --table trades --file data.csv

# List tables
exchange-db tables

# Table info
exchange-db info trades

# Create snapshot
exchange-db snapshot --output /backup/snap/

# Restore
exchange-db restore --input /backup/snap/
```

---

## Size and Duration Formats

### Size Format

Sizes can be specified with unit suffixes:

| Format | Example | Bytes |
|--------|---------|-------|
| Plain integer | `4096` | 4,096 |
| `KB` | `"4KB"` | 4,096 |
| `MB` | `"64MB"` | 67,108,864 |
| `GB` | `"1GB"` | 1,073,741,824 |
| `TB` | `"2TB"` | 2,199,023,255,552 |

### Duration Format

Durations can be specified with unit suffixes:

| Format | Example | Seconds |
|--------|---------|---------|
| `s` | `"30s"` | 30 |
| `m` | `"5m"` | 300 |
| `h` | `"1h"` | 3,600 |
| `d` | `"7d"` | 604,800 |
| `w` | `"2w"` | 1,209,600 |

---

## Example Configuration Files

### Minimal Development Configuration

```toml
[server]
data_dir = "./data"
log_level = "debug"
```

### Production Single-Node Configuration

```toml
[server]
data_dir = "/var/lib/exchangedb"
log_level = "info"

[http]
bind = "0.0.0.0:9000"
enabled = true

[pgwire]
bind = "0.0.0.0:8812"
enabled = true

[ilp]
bind = "0.0.0.0:9009"
enabled = true
batch_size = 5000

[storage]
wal_enabled = true
wal_max_segment_size = "128MB"
default_partition_by = "day"
mmap_page_size = "4KB"

[retention]
enabled = true
max_age = "90d"
check_interval = "1h"

[performance]
query_parallelism = 0
writer_commit_mode = "async"

[security]
auth_enabled = true
token = "your-production-token"
tls_cert = "/etc/exchangedb/server.crt"
tls_key = "/etc/exchangedb/server.key"
```

### Production Primary with Replication

```toml
[server]
data_dir = "/var/lib/exchangedb"
log_level = "info"

[http]
bind = "0.0.0.0:9000"

[pgwire]
bind = "0.0.0.0:8812"

[ilp]
bind = "0.0.0.0:9009"
batch_size = 10000

[storage]
wal_enabled = true
wal_max_segment_size = "256MB"
default_partition_by = "day"

[retention]
enabled = true
max_age = "365d"
check_interval = "6h"

[performance]
query_parallelism = 0
writer_commit_mode = "sync"

[security]
auth_enabled = true
token = "primary-token"
tls_cert = "/etc/exchangedb/server.crt"
tls_key = "/etc/exchangedb/server.key"

[replication]
mode = "primary"
```

### Production Replica Configuration

```toml
[server]
data_dir = "/var/lib/exchangedb"
log_level = "info"

[http]
bind = "0.0.0.0:9000"

[pgwire]
bind = "0.0.0.0:8812"

[ilp]
enabled = false

[storage]
wal_enabled = true

[performance]
query_parallelism = 0

[security]
auth_enabled = true
token = "replica-token"
tls_cert = "/etc/exchangedb/server.crt"
tls_key = "/etc/exchangedb/server.key"

[replication]
mode = "replica"
primary_addr = "primary.internal:9009"
```

### Docker Environment Variable Configuration

```bash
docker run -d --name exchangedb \
  -p 9000:9000 -p 8812:8812 -p 9009:9009 \
  -v exchangedb-data:/data \
  -e EXCHANGEDB_DATA_DIR=/data \
  -e EXCHANGEDB_LOG_LEVEL=info \
  -e EXCHANGEDB_HTTP_BIND=0.0.0.0:9000 \
  -e EXCHANGEDB_PGWIRE_BIND=0.0.0.0:8812 \
  -e EXCHANGEDB_ILP_BIND=0.0.0.0:9009 \
  -e EXCHANGEDB_ILP_BATCH_SIZE=5000 \
  -e EXCHANGEDB_WAL_ENABLED=true \
  -e EXCHANGEDB_QUERY_PARALLELISM=0 \
  -e EXCHANGEDB_WRITER_COMMIT_MODE=async \
  -e EXCHANGEDB_AUTH_ENABLED=true \
  -e EXCHANGEDB_TOKEN=your-token \
  -e EXCHANGEDB_RETENTION_ENABLED=true \
  -e EXCHANGEDB_RETENTION_MAX_AGE=90d \
  exchangedb
```
