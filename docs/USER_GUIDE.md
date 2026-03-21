# ExchangeDB User Guide

ExchangeDB is a high-performance time-series database built for financial exchanges and market data workloads. It provides three ingestion/query interfaces: an HTTP REST API, a PostgreSQL wire protocol (pgwire), and an InfluxDB Line Protocol (ILP) TCP endpoint.

---

## Table of Contents

1. [Getting Started](#getting-started)
   - [Installation](#installation)
   - [First Steps](#first-steps)
   - [Connecting Clients](#connecting-clients)
2. [SQL Reference](#sql-reference)
   - [Data Types](#data-types)
   - [CREATE TABLE](#create-table)
   - [INSERT](#insert)
   - [SELECT](#select)
   - [SAMPLE BY](#sample-by)
   - [FILL](#fill)
   - [LATEST ON](#latest-on)
   - [ASOF JOIN](#asof-join)
   - [GROUP BY](#group-by)
   - [ORDER BY and LIMIT](#order-by-and-limit)
   - [WHERE Clauses and Time Filters](#where-clauses-and-time-filters)
   - [ALTER TABLE](#alter-table)
   - [DROP TABLE](#drop-table)
   - [TRUNCATE TABLE](#truncate-table)
   - [Functions](#functions)
3. [Configuration Reference](#configuration-reference)
4. [Operations Guide](#operations-guide)
   - [Backup and Restore](#backup-and-restore)
   - [Monitoring with Prometheus](#monitoring-with-prometheus)
   - [Log Management](#log-management)
   - [Retention Policies](#retention-policies)
   - [Tiered Storage](#tiered-storage)
   - [Replication](#replication)
5. [Troubleshooting](#troubleshooting)

---

## Getting Started

### Installation

#### Build from Source

Prerequisites:
- Rust 1.85 or later
- A C linker (gcc/clang)
- OpenSSL development headers (on Debian/Ubuntu: `libssl-dev`)

```bash
git clone https://github.com/your-org/exchange-db.git
cd exchange-db
cargo build --release
```

The binary is at `target/release/exchange-db`.

```bash
# Install to your PATH
cp target/release/exchange-db /usr/local/bin/
```

#### Docker

Build and run with Docker:

```bash
docker build -t exchangedb .
docker run -d \
  --name exchangedb \
  -p 9000:9000 \
  -p 8812:8812 \
  -p 9009:9009 \
  -v exchangedb-data:/data \
  exchangedb
```

Or use Docker Compose:

```bash
docker compose up -d
```

The `docker-compose.yml` exposes all three ports (HTTP 9000, pgwire 8812, ILP 9009) and persists data in a named volume.

#### Verify the Installation

```bash
curl http://localhost:9000/api/v1/health
```

Expected response:

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_secs": 5.123
}
```

### First Steps

#### Create a Table

```bash
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE) TIMESTAMP(timestamp) PARTITION BY DAY"
  }'
```

#### Insert Data

Using SQL:

```bash
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO trades VALUES (now(), '\''BTC/USD'\'', 65000.0, 1.5)"
  }'
```

Using ILP (InfluxDB Line Protocol) over HTTP:

```bash
curl -X POST http://localhost:9000/api/v1/write \
  -d 'trades,symbol=BTC/USD price=65000.0,volume=1.5'
```

Using ILP over TCP (port 9009):

```bash
echo 'trades,symbol=BTC/USD price=65000.0,volume=1.5' | nc localhost 9009
```

#### Query Data

```bash
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM trades ORDER BY timestamp DESC LIMIT 10"}'
```

Response:

```json
{
  "columns": [
    {"name": "timestamp", "type": "Varchar"},
    {"name": "symbol", "type": "Varchar"},
    {"name": "price", "type": "Varchar"},
    {"name": "volume", "type": "Varchar"}
  ],
  "rows": [
    [1710513000000000000, "BTC/USD", 65000.0, 1.5]
  ],
  "timing_ms": 0.42
}
```

### Connecting Clients

#### psql (PostgreSQL Wire Protocol)

ExchangeDB speaks the PostgreSQL wire protocol on port 8812. Use any PostgreSQL client:

```bash
psql -h localhost -p 8812 -d exchangedb
```

Then run SQL directly:

```sql
SELECT symbol, avg(price), sum(volume)
FROM trades
WHERE timestamp > '2024-01-01'
GROUP BY symbol;
```

#### curl (HTTP REST API)

All SQL operations are available through the REST API:

```bash
# Query
curl -s -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT count() FROM trades"}'

# List tables
curl -s http://localhost:9000/api/v1/tables

# Table info
curl -s http://localhost:9000/api/v1/tables/trades

# Export as CSV
curl -s 'http://localhost:9000/api/v1/export?query=SELECT+*+FROM+trades+LIMIT+100&format=csv'

# Streaming NDJSON
curl -s 'http://localhost:9000/api/v1/query/stream?query=SELECT+*+FROM+trades'
```

#### Python

Using the `psycopg2` PostgreSQL driver:

```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=8812,
    dbname="exchangedb"
)
cur = conn.cursor()
cur.execute("SELECT symbol, avg(price) FROM trades GROUP BY symbol")
for row in cur.fetchall():
    print(row)
conn.close()
```

Using `requests` with the HTTP API:

```python
import requests

resp = requests.post(
    "http://localhost:9000/api/v1/query",
    json={"query": "SELECT * FROM trades LIMIT 5"}
)
data = resp.json()
for row in data["rows"]:
    print(row)
```

Using ILP for high-throughput ingestion:

```python
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost", 9009))

lines = [
    "trades,symbol=BTC/USD price=65000.0,volume=1.5",
    "trades,symbol=ETH/USD price=3200.0,volume=10.0",
]
for line in lines:
    sock.sendall((line + "\n").encode())

sock.close()
```

#### Node.js

```javascript
const { Client } = require('pg');

const client = new Client({
  host: 'localhost',
  port: 8812,
  database: 'exchangedb',
});

await client.connect();
const res = await client.query('SELECT * FROM trades LIMIT 10');
console.log(res.rows);
await client.end();
```

#### Go

```go
package main

import (
    "database/sql"
    "fmt"
    _ "github.com/lib/pq"
)

func main() {
    db, _ := sql.Open("postgres", "host=localhost port=8812 dbname=exchangedb sslmode=disable")
    defer db.Close()

    rows, _ := db.Query("SELECT symbol, price FROM trades LIMIT 10")
    defer rows.Close()

    for rows.Next() {
        var symbol string
        var price float64
        rows.Scan(&symbol, &price)
        fmt.Printf("%s: %.2f\n", symbol, price)
    }
}
```

---

## SQL Reference

### Data Types

| SQL Type          | Description                          | Size     |
|-------------------|--------------------------------------|----------|
| `BOOLEAN`         | True/false                           | 1 byte   |
| `TINYINT` / `I8`  | 8-bit signed integer                | 1 byte   |
| `SMALLINT` / `I16`| 16-bit signed integer               | 2 bytes  |
| `INT` / `I32`     | 32-bit signed integer               | 4 bytes  |
| `BIGINT` / `I64`  | 64-bit signed integer               | 8 bytes  |
| `FLOAT` / `F32`   | 32-bit IEEE 754 float               | 4 bytes  |
| `DOUBLE` / `F64`  | 64-bit IEEE 754 float               | 8 bytes  |
| `TIMESTAMP`       | Nanosecond-precision timestamp      | 8 bytes  |
| `VARCHAR`         | Variable-length string               | Variable |
| `SYMBOL`          | Interned string (low-cardinality)    | 4 bytes  |
| `UUID`            | 128-bit UUID                         | 16 bytes |
| `DATE`            | Calendar date                        | 4 bytes  |
| `CHAR`            | Single character                     | 2 bytes  |
| `IPV4`            | IPv4 address                         | 4 bytes  |
| `LONG256`         | 256-bit integer                      | 32 bytes |
| `GEOHASH`         | Geospatial hash                      | Variable |
| `BINARY`          | Binary data                          | Variable |

### CREATE TABLE

```sql
CREATE TABLE table_name (
    column1 TYPE,
    column2 TYPE,
    ...
) TIMESTAMP(timestamp_column) PARTITION BY {NONE | HOUR | DAY | WEEK | MONTH | YEAR};
```

The `TIMESTAMP()` clause designates which column is the primary time axis. The `PARTITION BY` clause controls how data is physically partitioned on disk.

**Examples:**

```sql
-- Market trades with daily partitions
CREATE TABLE trades (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    price DOUBLE,
    volume DOUBLE,
    side VARCHAR
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- Order book snapshots with hourly partitions
CREATE TABLE orderbook (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    level INT,
    bid_price DOUBLE,
    bid_size DOUBLE,
    ask_price DOUBLE,
    ask_size DOUBLE
) TIMESTAMP(timestamp) PARTITION BY HOUR;

-- Sensor data with monthly partitions
CREATE TABLE sensors (
    timestamp TIMESTAMP,
    device_id VARCHAR,
    temperature DOUBLE,
    humidity DOUBLE
) TIMESTAMP(timestamp) PARTITION BY MONTH;
```

#### INDEX Clause

Columns of type `SYMBOL` are automatically indexed for fast lookups. You can also create indexed columns via ILP ingestion, where tag columns are stored as indexed SYMBOLs.

### INSERT

```sql
INSERT INTO table_name VALUES (value1, value2, ...);
INSERT INTO table_name (col1, col2) VALUES (val1, val2);
```

**Examples:**

```sql
-- Insert with now() for the timestamp
INSERT INTO trades VALUES (now(), 'BTC/USD', 65000.0, 1.5, 'buy');

-- Insert with explicit nanosecond timestamp
INSERT INTO trades VALUES (1710513000000000000, 'ETH/USD', 3200.0, 10.0, 'sell');

-- Insert with named columns
INSERT INTO trades (timestamp, symbol, price, volume)
VALUES (now(), 'SOL/USD', 145.50, 100.0);
```

### SELECT

```sql
SELECT [columns | expressions | *]
FROM table_name
[WHERE conditions]
[GROUP BY columns]
[HAVING conditions]
[ORDER BY columns [ASC | DESC]]
[LIMIT n]
[OFFSET n];
```

**Examples:**

```sql
-- Select all columns
SELECT * FROM trades;

-- Select specific columns with alias
SELECT symbol, price AS last_price, volume
FROM trades
ORDER BY timestamp DESC
LIMIT 100;

-- Aggregation
SELECT symbol,
       count() AS trade_count,
       avg(price) AS avg_price,
       min(price) AS min_price,
       max(price) AS max_price,
       sum(volume) AS total_volume
FROM trades
WHERE timestamp > '2024-01-01'
GROUP BY symbol;

-- Distinct values
SELECT DISTINCT symbol FROM trades;

-- Subquery
SELECT * FROM trades
WHERE price > (SELECT avg(price) FROM trades WHERE symbol = 'BTC/USD');
```

### SAMPLE BY

Time-based downsampling. Aggregates rows into fixed time buckets.

```sql
SELECT [columns | aggregates]
FROM table_name
[WHERE conditions]
SAMPLE BY interval;
```

Supported intervals: `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks), `M` (months), `y` (years).

**Examples:**

```sql
-- 1-minute OHLCV candles
SELECT symbol,
       first(price) AS open,
       max(price) AS high,
       min(price) AS low,
       last(price) AS close,
       sum(volume) AS volume
FROM trades
WHERE timestamp > dateadd('d', -1, now())
SAMPLE BY 1m;

-- Hourly average temperature
SELECT device_id, avg(temperature), max(temperature), min(temperature)
FROM sensors
SAMPLE BY 1h;

-- 5-second trade bars
SELECT symbol, avg(price), sum(volume), count()
FROM trades
SAMPLE BY 5s;
```

### FILL

Used with `SAMPLE BY` to handle time buckets that have no data.

```sql
SELECT ...
FROM table_name
SAMPLE BY interval
FILL(strategy);
```

Strategies:
- `NONE` -- leave gaps (default)
- `NULL` -- fill with NULL
- `PREV` -- carry forward the previous value
- `LINEAR` -- linearly interpolate between surrounding values
- `value` -- fill with a constant (e.g., `FILL(0)`)

**Examples:**

```sql
-- Forward-fill missing candles
SELECT symbol, last(price) AS price
FROM trades
SAMPLE BY 1m
FILL(PREV);

-- Fill with zeros
SELECT symbol, sum(volume) AS volume
FROM trades
SAMPLE BY 1h
FILL(0);

-- Linear interpolation for sensor data
SELECT device_id, avg(temperature)
FROM sensors
SAMPLE BY 5m
FILL(LINEAR);
```

### LATEST ON

Returns the most recent row for each partition key value. Extremely efficient for getting "current state" queries.

```sql
SELECT *
FROM table_name
LATEST ON timestamp_column
PARTITION BY partition_column;
```

**Examples:**

```sql
-- Latest price per symbol
SELECT * FROM trades
LATEST ON timestamp PARTITION BY symbol;

-- Latest reading per sensor
SELECT * FROM sensors
LATEST ON timestamp PARTITION BY device_id;

-- Latest with filter
SELECT * FROM trades
LATEST ON timestamp PARTITION BY symbol
WHERE symbol IN ('BTC/USD', 'ETH/USD');
```

### ASOF JOIN

Joins two time-series tables by finding the closest matching timestamp. Essential for aligning trades with quotes, or any event-driven data fusion.

```sql
SELECT t.*, q.bid, q.ask
FROM table1 t
ASOF JOIN table2 q ON (t.match_column = q.match_column);
```

The ASOF JOIN matches each row from the left table with the row from the right table that has the closest timestamp less than or equal to the left row's timestamp.

**Examples:**

```sql
-- Align trades with the most recent quote
SELECT t.timestamp, t.symbol, t.price, t.volume,
       q.bid, q.ask, (q.ask - q.bid) AS spread
FROM trades t
ASOF JOIN quotes q ON (t.symbol = q.symbol);

-- Align sensor readings from two different devices
SELECT a.timestamp, a.temperature AS temp_indoor,
       b.temperature AS temp_outdoor
FROM sensors_indoor a
ASOF JOIN sensors_outdoor b;
```

### GROUP BY

```sql
SELECT columns, aggregates
FROM table_name
[WHERE conditions]
GROUP BY columns
[HAVING conditions];
```

**Examples:**

```sql
-- Trade statistics per symbol
SELECT symbol,
       count() AS trades,
       avg(price) AS avg_price,
       stddev(price) AS price_stddev,
       sum(volume) AS total_volume
FROM trades
GROUP BY symbol
HAVING count() > 100;

-- Hourly stats
SELECT symbol,
       hour(timestamp) AS hr,
       avg(price),
       sum(volume)
FROM trades
GROUP BY symbol, hour(timestamp);
```

### ORDER BY and LIMIT

```sql
SELECT * FROM table_name
ORDER BY column1 [ASC|DESC], column2 [ASC|DESC]
LIMIT count
OFFSET skip;
```

**Examples:**

```sql
-- Top 10 most volatile symbols
SELECT symbol, max(price) - min(price) AS range
FROM trades
GROUP BY symbol
ORDER BY range DESC
LIMIT 10;

-- Paginated results
SELECT * FROM trades
ORDER BY timestamp DESC
LIMIT 50 OFFSET 100;
```

### WHERE Clauses and Time Filters

```sql
-- Comparison operators
WHERE price > 65000 AND volume >= 1.0

-- String matching
WHERE symbol = 'BTC/USD'
WHERE symbol IN ('BTC/USD', 'ETH/USD')

-- Time range filters
WHERE timestamp > '2024-01-01'
WHERE timestamp > '2024-01-01T00:00:00Z' AND timestamp < '2024-02-01'
WHERE timestamp > dateadd('h', -24, now())
WHERE timestamp BETWEEN '2024-01-01' AND '2024-12-31'

-- NULL checks
WHERE price IS NOT NULL

-- Boolean
WHERE is_active = true
```

### ALTER TABLE

```sql
-- Add a column
ALTER TABLE trades ADD COLUMN fee DOUBLE;

-- Drop a column
ALTER TABLE trades DROP COLUMN fee;

-- Rename a column
ALTER TABLE trades RENAME COLUMN side TO trade_side;

-- Change column type
ALTER TABLE trades ALTER COLUMN volume TYPE BIGINT;
```

### DROP TABLE

```sql
DROP TABLE trades;
DROP TABLE IF EXISTS trades;
```

### TRUNCATE TABLE

```sql
TRUNCATE TABLE trades;
```

Removes all data from the table but keeps the schema.

### Functions

#### Aggregate Functions

| Function      | Description                              |
|---------------|------------------------------------------|
| `count()`     | Number of rows                           |
| `sum(col)`    | Sum of values                            |
| `avg(col)`    | Average of values                        |
| `min(col)`    | Minimum value                            |
| `max(col)`    | Maximum value                            |
| `first(col)`  | First value in time order                |
| `last(col)`   | Last value in time order                 |
| `stddev(col)` | Standard deviation                       |

#### Time Functions

| Function                    | Description                              |
|-----------------------------|------------------------------------------|
| `now()`                     | Current timestamp                        |
| `dateadd(unit, n, ts)`      | Add/subtract time from a timestamp       |
| `datediff(unit, ts1, ts2)`  | Difference between two timestamps        |
| `hour(ts)`                  | Extract hour                             |
| `day(ts)`                   | Extract day                              |
| `month(ts)`                 | Extract month                            |
| `year(ts)`                  | Extract year                             |

---

## Configuration Reference

ExchangeDB loads configuration from a TOML file (`exchange-db.toml`), with environment variable overrides and CLI flags taking highest precedence. The priority order is:

1. CLI flags (`--bind`, `--data-dir`, etc.)
2. Environment variables (`EXCHANGEDB_*`)
3. Configuration file (`exchange-db.toml`)
4. Built-in defaults

### Full Configuration File

```toml
[server]
data_dir = "./data"          # Root data directory
log_level = "info"           # Log level: trace, debug, info, warn, error
log_format = "text"          # Log format: "text" or "json"

[http]
bind = "0.0.0.0:9000"       # HTTP REST API bind address
enabled = true               # Enable/disable the HTTP server

[pgwire]
bind = "0.0.0.0:8812"       # PostgreSQL wire protocol bind address
enabled = true               # Enable/disable the pgwire server

[ilp]
bind = "0.0.0.0:9009"       # ILP TCP ingestion bind address
enabled = true               # Enable/disable the ILP server
batch_size = 1000            # Number of ILP lines to batch before flushing

[storage]
wal_enabled = true           # Enable Write-Ahead Log for durability
wal_max_segment_size = "64MB"  # Max WAL segment size before rotation
default_partition_by = "day" # Default partition strategy for new tables
mmap_page_size = "4KB"       # Memory-mapped page size

[retention]
enabled = false              # Enable automatic data retention
max_age = "30d"              # Maximum data age (e.g., "30d", "1y")
check_interval = "1h"        # How often to check for expired partitions

[performance]
query_parallelism = 0        # 0 = auto (number of CPU cores)
writer_commit_mode = "async" # "sync" or "async"

[tls]
enabled = false              # Enable TLS/HTTPS
cert_path = "cert.pem"       # Path to TLS certificate
key_path = "key.pem"         # Path to TLS private key

[replication]
role = "standalone"          # "standalone", "primary", or "replica"
primary_addr = ""            # For replicas: address of the primary
replica_addrs = []           # For primaries: list of replica addresses
sync_mode = "async"          # "async", "semi-sync", or "sync"

[cairo]
max_uncommitted_rows = 500000                   # Max uncommitted rows in memory
commit_lag = "10s"                              # Max time before auto-commit
o3_max_lag = "600s"                             # Out-of-order max lag
writer_data_append_page_size = "16MB"           # Writer append page size
reader_pool_max_segments = 5                    # Max reader pool segments
spin_lock_timeout = "5s"                        # Spin lock timeout
character_store_capacity = 1024                 # Character store capacity
character_store_sequence_pool_capacity = 64     # Sequence pool capacity
column_pool_capacity = 4096                     # Column pool capacity
compact_map_load_factor = 0.7                   # Compact map load factor
default_map_type = "fast"                       # "fast" or "compact"
default_symbol_cache_flag = true                # Enable symbol cache
default_symbol_capacity = 256                   # Default symbol capacity
file_operation_retry_count = 30                 # File operation retry count
inactive_reader_ttl = "120s"                    # Inactive reader TTL
inactive_writer_ttl = "120s"                    # Inactive writer TTL
index_value_block_size = 256                    # Index value block size
max_swap_file_count = 30                        # Max swap file count
mkdir_mode = 511                                # Directory creation mode (octal 0o777)
parallel_index_threshold = 100000               # Threshold for parallel indexing
snapshot_instance_id = ""                       # Snapshot instance identifier
sql_copy_buffer_size = "4MB"                    # SQL COPY buffer size
system_table_prefix = "sys."                    # System table prefix
volumes = []                                    # Additional storage volumes

[wal]
enabled = true                                  # Enable WAL
max_segment_size = "64MB"                       # Max WAL segment size
apply_table_time_quota = "30s"                  # Time quota for applying WAL to tables
purge_interval = "30s"                          # Interval for purging old WAL segments
segment_rollover_row_count = 200000             # Rows per WAL segment before rollover
squash_uncommitted_rows_multiplier = 20.0       # Multiplier for squashing uncommitted rows

[o3]
partition_split_min_size = "50MB"               # Min partition size before splitting
last_partition_max_splits = 20                  # Max splits for the latest partition
column_memory_size = "8MB"                      # Out-of-order column memory size

[memory]
max_per_query = "256MB"                         # Max memory per query
max_total = 0                                   # Max total memory (0 = unlimited)
sort_key_max_size = "2MB"                       # Max sort key size

[telemetry]
enabled = true                                  # Enable telemetry collection
queue_capacity = 512                            # Telemetry queue capacity
hide_tables = false                             # Hide table names in telemetry

[security]
auth_enabled = false                            # Enable authentication
rbac_enabled = false                            # Enable role-based access control
audit_enabled = false                           # Enable audit logging
password_min_length = 8                         # Minimum password length
session_timeout = "1h"                          # Session timeout
max_failed_login_attempts = 5                   # Max failed logins before lockout
lockout_duration = "15m"                        # Lockout duration after max failures

[cluster]
enabled = false                                 # Enable cluster mode
node_id = ""                                    # Unique node identifier
seed_nodes = []                                 # List of seed node addresses
heartbeat_interval = "5s"                       # Heartbeat interval
failure_threshold = 3                           # Failures before marking node down

[backup]
enabled = false                                 # Enable scheduled backups
schedule = "0 2 * * *"                          # Cron schedule (default: daily at 2 AM)
destination = ""                                # Backup destination path
retention_count = 7                             # Number of backups to retain
```

### Environment Variables

Every configuration option can be overridden with an environment variable. The naming convention is `EXCHANGEDB_` followed by the section and key in uppercase:

| Variable | Description | Default |
|----------|-------------|---------|
| `EXCHANGEDB_DATA_DIR` | Data directory | `./data` |
| `EXCHANGEDB_LOG_LEVEL` | Log level | `info` |
| `EXCHANGEDB_LOG_FORMAT` | Log format (`text` or `json`) | `text` |
| `EXCHANGEDB_HTTP_BIND` | HTTP bind address | `0.0.0.0:9000` |
| `EXCHANGEDB_HTTP_ENABLED` | Enable HTTP server | `true` |
| `EXCHANGEDB_PGWIRE_BIND` | pgwire bind address | `0.0.0.0:8812` |
| `EXCHANGEDB_PGWIRE_ENABLED` | Enable pgwire server | `true` |
| `EXCHANGEDB_ILP_BIND` | ILP bind address | `0.0.0.0:9009` |
| `EXCHANGEDB_ILP_ENABLED` | Enable ILP server | `true` |
| `EXCHANGEDB_ILP_BATCH_SIZE` | ILP batch size | `1000` |
| `EXCHANGEDB_WAL_ENABLED` | Enable WAL | `true` |
| `EXCHANGEDB_QUERY_PARALLELISM` | Query parallelism | `0` (auto) |
| `EXCHANGEDB_WRITER_COMMIT_MODE` | Writer commit mode | `async` |
| `EXCHANGEDB_TLS_ENABLED` | Enable TLS | `false` |
| `EXCHANGEDB_TLS_CERT_PATH` | TLS cert path | `cert.pem` |
| `EXCHANGEDB_TLS_KEY_PATH` | TLS key path | `key.pem` |
| `EXCHANGEDB_REPLICATION_ROLE` | Replication role | `standalone` |
| `EXCHANGEDB_REPLICATION_PRIMARY_ADDR` | Primary address (for replicas) | `` |
| `EXCHANGEDB_REPLICATION_SYNC_MODE` | Replication sync mode | `async` |

Additional environment variables exist for every `[cairo]`, `[wal]`, `[o3]`, `[memory]`, `[telemetry]`, `[security]`, `[cluster]`, and `[backup]` setting. The pattern is `EXCHANGEDB_<SECTION>_<KEY>` in uppercase (e.g., `EXCHANGEDB_CAIRO_MAX_UNCOMMITTED_ROWS`, `EXCHANGEDB_MEMORY_MAX_PER_QUERY`).

### CLI Flags

```bash
exchange-db server [OPTIONS]

Options:
  --config <PATH>      Path to configuration file (default: exchange-db.toml)
  --bind <ADDR>        HTTP bind address override
  --data-dir <PATH>    Data directory override
```

---

## Operations Guide

### Backup and Restore

#### CLI Snapshot

Create a point-in-time snapshot of all tables:

```bash
exchange-db snapshot --data-dir ./data --output /backups/snapshot-$(date +%Y%m%d)
```

Output:

```
Snapshot created successfully:
  Directory: /backups/snapshot-20240315
  Tables: trades, quotes, sensors
  Total size: 1048576 bytes
  Timestamp: 2024-03-15T10:30:00Z
```

Restore from a snapshot:

```bash
exchange-db restore --data-dir ./data --input /backups/snapshot-20240315
```

#### Scheduled Backups

Enable scheduled backups in the configuration file:

```toml
[backup]
enabled = true
schedule = "0 2 * * *"              # Daily at 2 AM
destination = "/backups/exchangedb"
retention_count = 7                  # Keep last 7 backups
```

### Monitoring with Prometheus

ExchangeDB exposes Prometheus-compatible metrics at `GET /metrics`:

```bash
curl http://localhost:9000/metrics
```

Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `exchangedb_queries_total` | Counter | Total queries executed |
| `exchangedb_queries_failed_total` | Counter | Total failed queries |
| `exchangedb_query_duration_seconds` | Histogram | Query execution time |
| `exchangedb_rows_written_total` | Counter | Total rows written |
| `exchangedb_rows_read_total` | Counter | Total rows read |
| `exchangedb_bytes_written_total` | Counter | Total bytes written |
| `exchangedb_bytes_read_total` | Counter | Total bytes read |
| `exchangedb_ilp_lines_total` | Counter | Total ILP lines ingested |
| `exchangedb_active_connections` | Gauge | Active HTTP connections |
| `exchangedb_tables_count` | Gauge | Number of tables |
| `exchangedb_wal_segments_total` | Counter | Total WAL segments |
| `exchangedb_wal_bytes_total` | Counter | Total WAL bytes |
| `exchangedb_slow_queries_total` | Counter | Total slow queries |
| `exchangedb_plan_cache_hits` | Counter | Plan cache hits |
| `exchangedb_plan_cache_misses` | Counter | Plan cache misses |

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: 'exchangedb'
    static_configs:
      - targets: ['localhost:9000']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

### Log Management

ExchangeDB uses structured logging via the `tracing` crate.

#### Log Format

Set the format via environment variable or config:

```bash
# Text format (default, human-readable)
EXCHANGEDB_LOG_FORMAT=text exchange-db server

# JSON format (machine-readable, for log aggregation)
EXCHANGEDB_LOG_FORMAT=json exchange-db server
```

#### Log Levels

Control verbosity with `RUST_LOG` or `EXCHANGEDB_LOG_LEVEL`:

```bash
# Set log level
RUST_LOG=info exchange-db server

# Fine-grained control
RUST_LOG=exchange_net=debug,exchange_query=trace exchange-db server
```

#### Runtime Log Level Changes

On Unix systems, send SIGHUP to reload the configuration file:

```bash
kill -HUP $(pidof exchange-db)
```

This will re-read `exchange-db.toml` and apply runtime-safe settings like log level without restarting.

### Retention Policies

Enable automatic data expiration to reclaim disk space:

```toml
[retention]
enabled = true
max_age = "30d"          # Delete partitions older than 30 days
check_interval = "1h"    # Check every hour
```

The retention system works at the partition level. When a partition is entirely older than `max_age`, it is dropped. Supported units for `max_age`: `s`, `m`, `h`, `d`, `w` (weeks).

### Tiered Storage

ExchangeDB supports additional storage volumes for tiered storage through the `cairo.volumes` configuration:

```toml
[cairo]
volumes = ["/mnt/fast-ssd", "/mnt/cold-hdd"]
```

Partitions can be moved between tiers based on age. Hot (recent) data stays on fast SSDs, while cold (historical) data is moved to cheaper storage.

### Replication

#### Primary-Replica Setup

Configure the primary node:

```toml
[replication]
role = "primary"
replica_addrs = ["10.0.0.2:9100", "10.0.0.3:9100"]
sync_mode = "async"    # or "semi-sync" or "sync"
```

Configure each replica:

```toml
[replication]
role = "replica"
primary_addr = "10.0.0.1:9100"
```

Replicas are automatically set to read-only mode. Writes sent to a replica return HTTP 403.

#### Sync Modes

- **async** -- Primary does not wait for replicas. Fastest, but replicas may lag.
- **semi-sync** -- Primary waits for at least one replica to acknowledge. Good balance.
- **sync** -- Primary waits for all replicas. Strongest durability, highest latency.

#### Monitoring Replication

```bash
# Check replication status via admin API
curl http://localhost:9000/admin/replication

# Response
{
  "role": "primary",
  "lag_bytes": 0,
  "lag_seconds": 0,
  "segments_shipped": 42,
  "segments_applied": 42
}
```

---

## Troubleshooting

### Cannot Connect to the Server

**Symptom:** `connection refused` on ports 9000, 8812, or 9009.

**Solutions:**
1. Verify the server is running: `curl http://localhost:9000/api/v1/health`
2. Check bind addresses -- the default is `0.0.0.0` (all interfaces). If you changed it to `127.0.0.1`, it only accepts local connections.
3. Check firewall rules: `sudo iptables -L -n` (Linux) or `sudo pfctl -sr` (macOS).
4. Verify port availability: `lsof -i :9000`

### WAL Recovery Fails on Startup

**Symptom:** The server exits with "WAL recovery failed" on startup.

**Solutions:**
1. Check disk space: WAL recovery needs enough space to replay segments.
2. Check file permissions on the data directory.
3. If data is corrupted, restore from a snapshot:
   ```bash
   exchange-db restore --data-dir ./data --input /backups/latest
   ```

### Slow Queries

**Symptom:** Queries take longer than expected.

**Solutions:**
1. Check the slow query log via the admin API:
   ```bash
   curl http://localhost:9000/admin/slow-queries
   ```
2. Use time filters -- always include `WHERE timestamp > ...` to limit partition scans.
3. Use `SYMBOL` type for columns you filter on frequently (they are indexed).
4. Increase `query_parallelism` if running on multi-core machines.
5. Check `SAMPLE BY` intervals -- larger intervals aggregate more data per bucket.

### Out of Memory

**Symptom:** Queries fail with "resource exhausted" errors.

**Solutions:**
1. Set memory limits in the configuration:
   ```toml
   [memory]
   max_per_query = "256MB"
   max_total = "4GB"
   ```
2. Use `LIMIT` to cap result sizes.
3. Use streaming queries (`/api/v1/query/stream`) for large result sets.
4. Reduce `cairo.max_uncommitted_rows` if ingestion is consuming too much memory.

### Table Not Found After Restart

**Symptom:** Tables disappear after restarting the server.

**Solutions:**
1. Verify `data_dir` is consistent across restarts. Check both the config file and `EXCHANGEDB_DATA_DIR`.
2. If using Docker, verify the volume is mounted: `docker inspect exchangedb | grep Mounts`
3. Check that the data directory contains table subdirectories with `_meta` files.

### ILP Ingestion Not Working

**Symptom:** Data sent via ILP (port 9009) is not appearing.

**Solutions:**
1. Verify the ILP server is enabled: `EXCHANGEDB_ILP_ENABLED=true`
2. Each ILP line must end with a newline (`\n`).
3. Check line format: `measurement,tag1=val1 field1=val1,field2=val2 [timestamp]`
4. Integer fields need an `i` suffix: `price=65000i`
5. String fields need quoting: `name="hello"`
6. Verify with: `echo 'test,tag=a value=1.0' | nc localhost 9009`

### Docker Container Keeps Restarting

**Symptom:** The container health check fails and the container restarts.

**Solutions:**
1. The health check uses `curl`, which is not installed in the slim container image. If you customized the Dockerfile, ensure `curl` or an alternative is available, or use a custom health check.
2. Check container logs: `docker logs exchangedb`
3. Verify the data volume has correct permissions.
4. Ensure nothing else is using ports 9000, 8812, or 9009.

### Permission Denied Errors

**Symptom:** API returns 403 Forbidden.

**Solutions:**
1. If the server is a replica, it is read-only. Send writes to the primary.
2. If authentication is enabled, include a valid `Authorization` header.
3. Check RBAC roles via the admin API:
   ```bash
   curl http://localhost:9000/admin/users
   curl http://localhost:9000/admin/roles
   ```
