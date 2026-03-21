# CLI Reference

ExchangeDB provides a comprehensive CLI for server operation, administration, debugging, and data management.

```
exchange-db <COMMAND> [OPTIONS]
```

---

## Table of Contents

- [Server & Data](#server--data)
  - [server](#exchange-db-server)
  - [sql](#exchange-db-sql)
  - [import](#exchange-db-import)
  - [tables](#exchange-db-tables)
  - [info](#exchange-db-info)
  - [snapshot](#exchange-db-snapshot)
  - [restore](#exchange-db-restore)
- [Configuration Management](#configuration-management)
  - [config show](#exchange-db-config-show)
  - [config validate](#exchange-db-config-validate)
  - [config generate](#exchange-db-config-generate)
- [Health Checks & Diagnostics](#health-checks--diagnostics)
  - [check all](#exchange-db-check-all)
  - [check wal](#exchange-db-check-wal)
  - [check partitions](#exchange-db-check-partitions)
  - [check metadata](#exchange-db-check-metadata)
  - [check disk-usage](#exchange-db-check-disk-usage)
  - [status](#exchange-db-status)
- [Replication Management](#replication-management)
  - [replication status](#exchange-db-replication-status)
  - [replication promote](#exchange-db-replication-promote)
  - [replication demote](#exchange-db-replication-demote)
- [Debugging](#debugging)
  - [debug wal-inspect](#exchange-db-debug-wal-inspect)
  - [debug partition-info](#exchange-db-debug-partition-info)
  - [debug column-dump](#exchange-db-debug-column-dump)
  - [debug diagnostics](#exchange-db-debug-diagnostics)
- [Maintenance](#maintenance)
  - [compact](#exchange-db-compact)
  - [version](#exchange-db-version)

---

## Server & Data

### `exchange-db server`

Start the database server with all configured protocols (HTTP, pgwire, ILP).

```bash
# Default: all interfaces, default ports
exchange-db server

# Custom config file
exchange-db server --config /etc/exchangedb/exchange-db.toml

# Override bind address and data directory
exchange-db server --bind 0.0.0.0:9000 --data-dir /var/lib/exchangedb
```

| Flag | Description | Default |
|------|-------------|---------|
| `--config <PATH>` | Configuration file path | `exchange-db.toml` |
| `--bind <HOST:PORT>` | HTTP bind address (overrides config) | from config |
| `--data-dir <PATH>` | Data directory (overrides config) | from config |

On startup the server:
1. Loads configuration (file + env vars + CLI flags)
2. Runs WAL crash recovery
3. Starts the replication manager (if configured)
4. Starts background job scheduler (WAL cleanup, checkpoint, retention, etc.)
5. Starts SIGHUP watcher for hot config reload (Unix)
6. Starts all protocol listeners concurrently

### `exchange-db sql`

Execute a SQL query directly and print results as an ASCII table.

```bash
exchange-db sql "SELECT * FROM trades LIMIT 10"

exchange-db sql "SELECT symbol, count(*), avg(price) FROM trades GROUP BY symbol"

exchange-db sql "CREATE TABLE quotes (
  timestamp TIMESTAMP, symbol SYMBOL, bid DOUBLE, ask DOUBLE
) TIMESTAMP(timestamp) PARTITION BY DAY"
```

| Flag | Description | Default |
|------|-------------|---------|
| `--data-dir <PATH>` | Data directory | `./data` |

### `exchange-db import`

Import a CSV file into a table. Creates the table automatically if it doesn't exist, detecting column types from the CSV data.

```bash
exchange-db import --table trades --file data.csv

exchange-db import --table trades --file data.csv --data-dir /var/lib/exchangedb
```

| Flag | Description | Default |
|------|-------------|---------|
| `--table <NAME>` | Target table name (required) | |
| `--file <PATH>` | Path to CSV file (required) | |
| `--data-dir <PATH>` | Data directory | `./data` |

### `exchange-db tables`

List all tables in the database.

```bash
exchange-db tables
exchange-db tables --data-dir /var/lib/exchangedb
```

### `exchange-db info`

Show detailed information about a table: schema, column types, partition strategy, row count.

```bash
exchange-db info trades
```

Example output:

```
Table: trades
Partition by: Day
Version: 1
Designated timestamp: timestamp

Columns (5):
  timestamp            TIMESTAMP   [timestamp]
  symbol               SYMBOL      [indexed]
  price                DOUBLE
  volume               DOUBLE
  side                 SYMBOL      [indexed]

Row count: 1048576
```

### `exchange-db snapshot`

Create a point-in-time snapshot of all tables for backup.

```bash
exchange-db snapshot --output /backup/exchangedb-2024-03-01/
```

### `exchange-db restore`

Restore the database from a snapshot.

```bash
exchange-db restore --input /backup/exchangedb-2024-03-01/
```

---

## Configuration Management

### `exchange-db config show`

Show the effective configuration after merging file + environment variables + defaults.

```bash
# TOML format (default)
exchange-db config show

# JSON format
exchange-db config show --format json

# From a specific config file
exchange-db config show --config /etc/exchangedb/exchange-db.toml
```

| Flag | Description | Default |
|------|-------------|---------|
| `--config <PATH>` | Configuration file | `exchange-db.toml` |
| `--format <FMT>` | Output format: `toml` or `json` | `toml` |

### `exchange-db config validate`

Validate a configuration file for syntax errors, invalid addresses, missing TLS certificates, etc.

```bash
exchange-db config validate --config exchange-db.toml
```

Example output:

```
Configuration parsed with 1 warning(s):
  WARNING: tls.cert_path: file not found: cert.pem
```

### `exchange-db config generate`

Generate a complete reference configuration file with all defaults and comments.

```bash
# Print to stdout
exchange-db config generate

# Write to file
exchange-db config generate --output exchange-db.toml
```

---

## Health Checks & Diagnostics

### `exchange-db check all`

Run all health checks: data directory, disk space, metadata, WAL, partitions.

```bash
exchange-db check all
exchange-db check all --data-dir /var/lib/exchangedb
```

Example output:

```
Running all checks on: ./data

[OK] Data directory exists
[OK] Disk usage: 23.4% (45.23 GB / 193.12 GB)
[OK] Found 3 table(s)
[OK] trades: metadata valid (5 columns, partition_by=Day)
[OK] trades: 12 WAL segment(s)
[OK] trades: 30 partition(s), 15728640 row(s)
[OK] quotes: metadata valid (4 columns, partition_by=Day)
[OK] quotes: 3 WAL segment(s)
[OK] quotes: 14 partition(s), 2097152 row(s)
[OK] ohlcv: metadata valid (7 columns, partition_by=Hour)
[WARN] ohlcv: 145 WAL segments (consider compacting)
[OK] ohlcv: 720 partition(s), 8388608 row(s)

All checks passed
```

### `exchange-db check wal`

Check WAL integrity and segment count for tables.

```bash
# All tables
exchange-db check wal

# Specific table
exchange-db check wal --table trades
```

### `exchange-db check partitions`

Verify partition integrity: row counts, sizes, column files.

```bash
exchange-db check partitions
exchange-db check partitions --table trades
```

Example output:

```
Table: trades
  PARTITION                          ROWS         SIZE
  2024-03-01                       524288    42.00 MB
  2024-03-02                       524288    42.00 MB
  2024-03-03                       262144    21.00 MB
  TOTAL                           1310720   105.00 MB
```

### `exchange-db check metadata`

Check metadata consistency across all tables.

```bash
exchange-db check metadata
```

### `exchange-db check disk-usage`

Show disk space usage broken down by table, with data vs WAL separation.

```bash
exchange-db check disk-usage
```

Example output:

```
TABLE                          DATA          WAL        TOTAL
--------------------------------------------------------------------
trades                     105.00 MB     12.50 MB   117.50 MB
quotes                      28.00 MB      3.20 MB    31.20 MB
ohlcv                      840.00 MB    156.00 MB   996.00 MB
--------------------------------------------------------------------
TOTAL                      973.00 MB    171.70 MB     1.12 GB
```

### `exchange-db status`

Check the status of a running server by querying its HTTP endpoints.

```bash
exchange-db status
exchange-db status --host http://my-server:9000
```

Example output:

```
Server: HEALTHY
  status: "ok"
  uptime: 86400
  version: "0.1.0"

Tables: 3
  - trades
  - quotes
  - ohlcv
```

---

## Replication Management

### `exchange-db replication status`

Show the current replication configuration and status.

```bash
exchange-db replication status
exchange-db replication status --config /etc/exchangedb/exchange-db.toml
```

Example output (primary):

```
Replication Status
  Role:                primary
  Sync mode:           async
  Replication port:    19100
  Failover enabled:    false
  Replicas:
    - 10.0.0.2:19100
    - 10.0.0.3:19100
```

### `exchange-db replication promote`

Promote a replica to primary. Use this during manual failover.

```bash
exchange-db replication promote
```

After promotion, update the configuration file:
```toml
[replication]
role = "primary"
```

### `exchange-db replication demote`

Demote a primary to replica and point it at a new primary.

```bash
exchange-db replication demote --new-primary 10.0.0.2:19100
```

---

## Debugging

### `exchange-db debug wal-inspect`

Inspect WAL segments for a table: segment count, sizes, and optionally column data files.

```bash
# Summary view
exchange-db debug wal-inspect trades

# Verbose: show column files in each segment
exchange-db debug wal-inspect trades --verbose
```

Example output:

```
WAL for table 'trades': 3 segment(s)

  wal-0/
    Total size: 4.20 MB
    Events file: 128 B
  wal-1/
    Total size: 4.18 MB
    Events file: 128 B
  wal-2/
    Total size: 2.10 MB
    Events file: 64 B
```

### `exchange-db debug partition-info`

Show detailed partition information including individual column file sizes.

```bash
exchange-db debug partition-info trades
```

### `exchange-db debug column-dump`

Dump raw column values for debugging data issues.

```bash
# Dump first 20 values from all partitions
exchange-db debug column-dump trades --column price

# Specific partition, more values
exchange-db debug column-dump trades --column price --partition 2024-03-01 --limit 50
```

Example output:

```
2024-03-01/price.d (DOUBLE, 4.00 MB)
  [     0] 65000.5
  [     1] 65001.2
  [     2] 64998.7
  ...
  ... and 524285 more values
```

### `exchange-db debug diagnostics`

Fetch server diagnostics from a running instance via the HTTP endpoint.

```bash
exchange-db debug diagnostics
exchange-db debug diagnostics --host http://my-server:9000
```

---

## Maintenance

### `exchange-db compact`

Compact WAL segments by merging them into the column store. Reclaims disk space.

```bash
# Compact all tables
exchange-db compact

# Compact specific table
exchange-db compact --table trades

# Preview what would be compacted
exchange-db compact --dry-run
```

### `exchange-db version`

Print version and build information.

```bash
exchange-db version
```

Example output:

```
ExchangeDB v0.1.0
  Rust edition: 2024
  Min Rust version: 1.85
  Target: aarch64
  OS: macos
  Profile: release
```

---

## Environment Variables

All configuration can also be set via environment variables with the `EXCHANGEDB_` prefix. See [Configuration Reference](CONFIGURATION_COMPLETE.md) for the full list.

Common examples:

```bash
# Set log level
EXCHANGEDB_LOG_LEVEL=debug exchange-db server

# JSON structured logging
EXCHANGEDB_LOG_FORMAT=json exchange-db server

# Custom data directory
EXCHANGEDB_DATA_DIR=/var/lib/exchangedb exchange-db server
```

## Hot Config Reload

On Unix systems, send `SIGHUP` to reload runtime-safe settings without restarting:

```bash
kill -HUP $(pidof exchange-db)
```

Currently reloadable: log level. Other settings require a restart.
