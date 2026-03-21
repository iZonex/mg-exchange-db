# ExchangeDB

**High-performance columnar time-series database for financial exchanges, written in Rust.**

![Lines of Rust](https://img.shields.io/badge/Rust-~200%2C000_lines-blue)
![Tests](https://img.shields.io/badge/tests-~26%2C500_passing-brightgreen)
![License](https://img.shields.io/badge/license-Apache--2.0-orange)
![Rust](https://img.shields.io/badge/rust-1.85+-dea584)

ExchangeDB is a columnar time-series database optimized for exchange workloads: tick data, OHLCV bars, order book snapshots, trades, and market events. It implements a full SQL engine with 1,198+ scalar functions, 120+ aggregates (including financial indicators like VWAP, EMA, RSI, and Bollinger Bands), 124 cursor-based execution strategies, and wire-compatible protocols for PostgreSQL clients, InfluxDB Line Protocol ingestion, HTTP REST, and WebSocket streaming. Built in Rust with zero-cost abstractions, no garbage collector, and predictable p99 latency.

---

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Docker](#docker)
- [CLI Usage](#cli-usage)
- [SQL Reference](#sql-reference)
- [Configuration](#configuration)
- [Client Libraries](#client-libraries)
- [Architecture](#architecture)
- [Benchmarks](#benchmarks)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

---

## Features

### Why ExchangeDB over QuestDB?

| Aspect | QuestDB (Java) | ExchangeDB (Rust) |
|--------|---------------|-------------------|
| GC pauses | Stop-the-world pauses (ms-s) | No GC, deterministic memory management |
| Memory control | Off-heap via Unsafe (workaround) | Native control, no JNI overhead |
| Concurrency safety | Manual lock-free via Unsafe | Compile-time `Send`/`Sync` guarantees |
| SIMD | JNI or JIT | Native intrinsics, portable_simd |
| Tail latency | p99 spikes from GC | Predictable p99 |
| Binary size | JVM + JAR (~200 MB) | Single static binary (~10 MB) |
| Financial aggregates | 6 functions | 10+ (VWAP, EMA, SMA, RSI, ATR, MACD, Bollinger, Drawdown) |

### Performance Highlights

- **18.80 M rows/s** batch write throughput (1M rows, columnar)
- **590.70 M elements/s** column read throughput (mmap, zero-copy)
- **4.49 G elements/s** SIMD-accelerated aggregation (3.97x over scalar)
- **163.64 M ticks/s** OHLCV aggregation for 1-second bars
- **61.05 M rows/s** multi-partition GROUP BY (30 partitions)
- **1.19 G rows/s** partition-pruned time-range query (TSBS max-CPU-12h)
- **55x speedup** from partition pruning (2.53 ms down to 45.5 us)
- **1.93x speedup** from parallel query execution across partitions

### SQL Compatibility

ExchangeDB supports a comprehensive SQL dialect compatible with PostgreSQL clients and QuestDB extensions:

- **DDL**: CREATE TABLE, ALTER TABLE (ADD/DROP/RENAME/SET TYPE COLUMN, DETACH/ATTACH/SQUASH PARTITION), DROP TABLE, TRUNCATE TABLE, CREATE/DROP MATERIALIZED VIEW
- **DML**: INSERT, INSERT INTO ... SELECT, INSERT ON CONFLICT, UPDATE, DELETE, MERGE INTO, COPY TO/FROM, VACUUM
- **Queries**: SELECT, WHERE, ORDER BY, GROUP BY (with GROUPING SETS, ROLLUP, CUBE), HAVING, DISTINCT, LIMIT/OFFSET, CTEs (WITH), subqueries, set operations (UNION/INTERSECT/EXCEPT)
- **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, ASOF, LATERAL, SEMI, ANTI, MARK, BAND (10 join types)
- **Time-series**: SAMPLE BY with FILL (NONE/NULL/PREV/LINEAR/constant), LATEST ON ... PARTITION BY, ALIGN TO CALENDAR
- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTH_VALUE, NTILE, PERCENT_RANK, CUME_DIST, running aggregates
- **Other**: CASE WHEN, PIVOT/UNPIVOT, EXPLAIN/EXPLAIN ANALYZE, SHOW TABLES/COLUMNS/CREATE TABLE

### Protocols

| Protocol | Port | Purpose |
|----------|------|---------|
| HTTP REST API | 9000 | Query, import/export, admin, health, metrics |
| Web Console | 9000 | Built-in SQL editor UI (dark theme) |
| WebSocket | 9000 | Real-time streaming subscriptions |
| PostgreSQL Wire | 8812 | Compatible with psql, DBeaver, Grafana, any PG client |
| ILP/TCP | 9009 | High-throughput InfluxDB Line Protocol ingestion |
| ILP/UDP | 9009 | Fire-and-forget ingestion |

---

## Quick Start

### Prerequisites

- Rust 1.85+ (install via [rustup](https://rustup.rs/))

### Build from Source

```bash
git clone https://github.com/iZonex/mg-exchange-db.git
cd mg-exchange-db
cargo build --release
```

### Start the Server

```bash
# Start with all protocols enabled (HTTP :9000, pgwire :8812, ILP :9009)
./target/release/exchange-db server

# Start with a custom config file
./target/release/exchange-db server --config exchange-db.toml

# Start with custom bind address and data directory
./target/release/exchange-db server --bind 127.0.0.1:9000 --data-dir /var/lib/exchangedb
```

### Connect via psql

```bash
psql -h localhost -p 8812

# Create a table
CREATE TABLE trades (
  timestamp TIMESTAMP, symbol SYMBOL, price DOUBLE, volume DOUBLE
) TIMESTAMP(timestamp) PARTITION BY DAY;

# Insert data
INSERT INTO trades VALUES ('2024-03-01T10:00:00Z', 'BTC/USD', 65000.0, 1.5);

# Query
SELECT symbol, avg(price), sum(volume) FROM trades SAMPLE BY 1h;
```

### Connect via HTTP

```bash
# Execute a query
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT 1"}'

# List tables
curl http://localhost:9000/api/v1/tables

# Health check
curl http://localhost:9000/health

# Export as CSV
curl 'http://localhost:9000/api/v1/export?query=SELECT+*+FROM+trades+LIMIT+100'
```

### Ingest via ILP

```bash
# Send data using InfluxDB Line Protocol over TCP
echo "trades,symbol=BTC/USD price=65000.0,volume=1.5 $(date +%s)000000000" | nc localhost 9009

# Bulk ingestion
cat data.ilp | nc localhost 9009
```

### Open the Web Console

Navigate to [http://localhost:9000](http://localhost:9000) in your browser to access the built-in SQL editor with table browser and result viewer.

---

## Docker

### Using Docker Compose (recommended)

```bash
docker compose up -d
```

This starts ExchangeDB with persistent storage, health checks, and all three ports exposed.

### Using Docker Directly

```bash
# Build the image
docker build -t exchangedb .

# Run with persistent volume
docker run -d --name exchangedb \
  -p 9000:9000 \
  -p 8812:8812 \
  -p 9009:9009 \
  -v exchangedb-data:/data \
  exchangedb
```

### Docker Environment Variables

All configuration can be passed via environment variables:

```bash
docker run -d --name exchangedb \
  -p 9000:9000 -p 8812:8812 -p 9009:9009 \
  -v exchangedb-data:/data \
  -e EXCHANGEDB_LOG_LEVEL=debug \
  -e EXCHANGEDB_WAL_ENABLED=true \
  -e EXCHANGEDB_QUERY_PARALLELISM=4 \
  exchangedb
```

---

## CLI Usage

```bash
# Start the server
exchange-db server
exchange-db server --config exchange-db.toml --data-dir /var/lib/exchangedb

# Execute SQL
exchange-db sql "SELECT * FROM trades LIMIT 10"
exchange-db sql "SELECT symbol, avg(price) FROM trades GROUP BY symbol"

# Import CSV
exchange-db import --table trades --file data.csv

# List tables / inspect schema
exchange-db tables
exchange-db info trades

# Backup and restore
exchange-db snapshot --output /backup/2024-03-01/
exchange-db restore --input /backup/2024-03-01/

# Generate a fully commented reference config
exchange-db config generate --output exchange-db.toml
```

ExchangeDB also includes admin commands for health checks (`check`), replication management (`replication`), WAL inspection (`debug`), and maintenance (`compact`). See the full [CLI Reference](docs/CLI_REFERENCE.md).

---

## SQL Reference

### Data Definition Language (DDL)

#### CREATE TABLE

```sql
-- Basic table with designated timestamp and partitioning
CREATE TABLE trades (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    price DOUBLE,
    volume DOUBLE,
    side SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- With IF NOT EXISTS
CREATE TABLE IF NOT EXISTS trades (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    price DOUBLE
) TIMESTAMP(timestamp) PARTITION BY HOUR;
```

Partition strategies: `NONE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `YEAR`.

#### ALTER TABLE

```sql
-- Add a column
ALTER TABLE trades ADD COLUMN fee DOUBLE;

-- Drop a column
ALTER TABLE trades DROP COLUMN fee;

-- Rename a column
ALTER TABLE trades RENAME COLUMN side TO direction;

-- Change column type
ALTER TABLE trades SET TYPE volume FLOAT;

-- Partition management
ALTER TABLE trades DETACH PARTITION '2024-01-01';
ALTER TABLE trades ATTACH PARTITION '2024-01-01';
ALTER TABLE trades SQUASH PARTITIONS '2024-01-01' '2024-01-02';
```

#### DROP TABLE / TRUNCATE TABLE

```sql
DROP TABLE trades;
DROP TABLE IF EXISTS trades;
TRUNCATE TABLE trades;
```

#### Materialized Views

```sql
CREATE MATERIALIZED VIEW ohlcv_1h AS
  SELECT symbol,
    first(price) AS open, max(price) AS high,
    min(price) AS low, last(price) AS close,
    sum(volume) AS volume
  FROM trades SAMPLE BY 1h;

REFRESH MATERIALIZED VIEW ohlcv_1h;
DROP MATERIALIZED VIEW ohlcv_1h;
```

### Data Manipulation Language (DML)

#### INSERT

```sql
-- Single row
INSERT INTO trades VALUES ('2024-03-01T10:00:00Z', 'BTC/USD', 65000.0, 1.5, 'buy');

-- Multiple rows
INSERT INTO trades VALUES
  ('2024-03-01T10:00:00Z', 'BTC/USD', 65000.0, 1.5, 'buy'),
  ('2024-03-01T10:00:01Z', 'ETH/USD', 3400.0, 10.0, 'sell');

-- Insert from SELECT
INSERT INTO trades_archive SELECT * FROM trades WHERE timestamp < '2024-01-01';

-- Upsert (INSERT ON CONFLICT)
INSERT INTO trades VALUES ('2024-03-01T10:00:00Z', 'BTC/USD', 65100.0, 1.5, 'buy')
  ON CONFLICT DO UPDATE SET price = EXCLUDED.price;
```

#### UPDATE / DELETE

```sql
UPDATE trades SET price = 65100.0 WHERE symbol = 'BTC/USD' AND timestamp = '2024-03-01T10:00:00Z';
DELETE FROM trades WHERE timestamp < '2024-01-01';
```

#### MERGE

```sql
MERGE INTO trades t
USING new_trades n ON t.timestamp = n.timestamp AND t.symbol = n.symbol
WHEN MATCHED THEN UPDATE SET price = n.price, volume = n.volume
WHEN NOT MATCHED THEN INSERT VALUES (n.timestamp, n.symbol, n.price, n.volume, n.side);
```

#### COPY

```sql
COPY trades TO '/tmp/trades.csv' WITH (FORMAT CSV, HEADER TRUE);
COPY trades FROM '/tmp/trades.csv' WITH (FORMAT CSV, HEADER TRUE);
```

### Queries

#### SELECT

```sql
-- Basic query with filtering
SELECT symbol, price, volume
FROM trades
WHERE timestamp BETWEEN '2024-01-01' AND '2024-03-01'
  AND symbol IN ('BTC/USD', 'ETH/USD')
  AND price > 50000
ORDER BY timestamp DESC
LIMIT 100 OFFSET 50;

-- Expressions and aliases
SELECT symbol,
       price * volume AS notional,
       CASE WHEN price > 60000 THEN 'high' ELSE 'low' END AS tier
FROM trades;

-- Subqueries
SELECT * FROM trades
WHERE price > (SELECT avg(price) FROM trades WHERE symbol = 'BTC/USD');

-- EXISTS
SELECT DISTINCT symbol FROM trades t
WHERE EXISTS (SELECT 1 FROM quotes q WHERE q.symbol = t.symbol);
```

#### GROUP BY / HAVING

```sql
-- Standard aggregation
SELECT symbol, count(*), avg(price), sum(volume), min(price), max(price)
FROM trades
GROUP BY symbol
HAVING count(*) > 1000
ORDER BY sum(volume) DESC;

-- Advanced grouping
SELECT symbol, extract(hour FROM timestamp) AS hour, avg(price)
FROM trades
GROUP BY ROLLUP(symbol, extract(hour FROM timestamp));

SELECT symbol, side, count(*)
FROM trades
GROUP BY CUBE(symbol, side);
```

#### SAMPLE BY (Time Bucketing)

ExchangeDB's time-series extension for bucketed aggregation:

```sql
-- 1-hour OHLCV bars
SELECT symbol,
    first(price) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price) AS close,
    sum(volume) AS volume
FROM trades
WHERE symbol = 'BTC/USD'
SAMPLE BY 1h;

-- With FILL to handle gaps
SELECT symbol, avg(price), sum(volume)
FROM trades
SAMPLE BY 1h FILL(PREV);

-- Fill modes: NONE, NULL, PREV, LINEAR, or a constant value
SELECT avg(price) FROM trades SAMPLE BY 5m FILL(LINEAR);
SELECT avg(price) FROM trades SAMPLE BY 1d FILL(0);

-- Calendar-aligned buckets
SELECT avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR;
SELECT avg(price) FROM trades SAMPLE BY 1h ALIGN TO FIRST OBSERVATION;
```

Supported intervals: seconds (`s`), minutes (`m`), hours (`h`), days (`d`), weeks (`w`), months (`M`), years (`y`).

#### LATEST ON

Get the most recent row per partition key:

```sql
-- Latest trade per symbol
SELECT * FROM trades
LATEST ON timestamp PARTITION BY symbol;

-- Latest trade per symbol and side
SELECT * FROM trades
LATEST ON timestamp PARTITION BY symbol, side;
```

#### JOINs

```sql
-- INNER JOIN
SELECT t.symbol, t.price, q.bid, q.ask
FROM trades t
INNER JOIN quotes q ON t.symbol = q.symbol AND t.timestamp = q.timestamp;

-- LEFT / RIGHT / FULL OUTER JOIN
SELECT t.*, q.bid, q.ask
FROM trades t
LEFT JOIN quotes q ON t.symbol = q.symbol;

-- CROSS JOIN
SELECT * FROM symbols CROSS JOIN timeframes;

-- ASOF JOIN (point-in-time temporal join)
SELECT t.timestamp, t.symbol, t.price, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q ON (t.symbol = q.symbol);

-- LATERAL JOIN
SELECT t.symbol, l.*
FROM (SELECT DISTINCT symbol FROM trades) t,
LATERAL (SELECT * FROM trades WHERE symbol = t.symbol ORDER BY timestamp DESC LIMIT 5) l;

-- SEMI JOIN / ANTI JOIN
SELECT * FROM trades t
WHERE EXISTS (SELECT 1 FROM watchlist w WHERE w.symbol = t.symbol);

SELECT * FROM trades t
WHERE NOT EXISTS (SELECT 1 FROM blacklist b WHERE b.symbol = t.symbol);
```

#### Common Table Expressions (CTEs)

```sql
WITH recent AS (
    SELECT * FROM trades WHERE timestamp > dateadd('d', -7, now())
),
top_symbols AS (
    SELECT symbol, sum(volume) AS total_vol
    FROM recent
    GROUP BY symbol
    ORDER BY total_vol DESC
    LIMIT 10
)
SELECT r.* FROM recent r
JOIN top_symbols ts ON r.symbol = ts.symbol;
```

#### Set Operations

```sql
SELECT symbol FROM exchange_a
UNION ALL
SELECT symbol FROM exchange_b;

SELECT symbol FROM exchange_a
INTERSECT
SELECT symbol FROM exchange_b;

SELECT symbol FROM all_symbols
EXCEPT
SELECT symbol FROM delisted;
```

#### PIVOT / UNPIVOT

```sql
SELECT * FROM (
    SELECT symbol, side, volume FROM trades
)
PIVOT (sum(volume) FOR side IN ('buy', 'sell'));
```

#### EXPLAIN

```sql
EXPLAIN SELECT * FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h;
EXPLAIN ANALYZE SELECT symbol, avg(price) FROM trades GROUP BY symbol;
```

### Window Functions

```sql
SELECT symbol, timestamp, price,
    -- Ranking
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp) AS rn,
    RANK() OVER (PARTITION BY symbol ORDER BY price DESC) AS price_rank,
    DENSE_RANK() OVER (PARTITION BY symbol ORDER BY price DESC) AS dense_rank,

    -- Offset
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev_price,
    LEAD(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) AS next_price,

    -- Running aggregates
    AVG(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS ma10,
    SUM(volume) OVER (PARTITION BY symbol ORDER BY timestamp) AS cumulative_vol,

    -- Distribution
    NTILE(4) OVER (ORDER BY price) AS quartile,
    PERCENT_RANK() OVER (ORDER BY price) AS pct_rank,
    CUME_DIST() OVER (ORDER BY price) AS cume_dist
FROM trades;
```

### Aggregate Functions

#### Standard Aggregates

`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `FIRST`, `LAST`, `COUNT_DISTINCT`, `STRING_AGG`, `BOOL_AND`, `BOOL_OR`, `ARRAY_AGG`

#### Statistical Aggregates

`STDDEV`, `VARIANCE`, `MEDIAN`, `PERCENTILE_CONT`, `PERCENTILE_DISC`, `MODE`, `CORR`, `COVAR_POP`, `COVAR_SAMP`, `REGR_SLOPE`, `REGR_INTERCEPT`

#### Financial Aggregates

```sql
-- Volume-weighted average price
SELECT symbol, vwap(price, volume) FROM trades GROUP BY symbol;

-- Moving averages
SELECT symbol, ema(price, 20) AS ema20, sma(price, 50) AS sma50 FROM trades GROUP BY symbol;

-- Relative Strength Index
SELECT symbol, rsi(price, 14) FROM trades GROUP BY symbol;

-- Bollinger Bands
SELECT symbol,
    bollinger_upper(price, 20, 2.0) AS upper,
    bollinger_lower(price, 20, 2.0) AS lower
FROM trades GROUP BY symbol;

-- Average True Range
SELECT symbol, atr(high, low, close, 14) FROM ohlcv GROUP BY symbol;

-- Maximum drawdown
SELECT symbol, drawdown(price) FROM trades GROUP BY symbol;
```

#### Compensated Summation

`KSUM` (Kahan summation), `NSUM` (Neumaier summation) for numerically stable aggregation over large floating-point datasets.

### Scalar Functions (1,198+)

#### String Functions
`length`, `substr`, `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `replace`, `concat`, `left`, `right`, `reverse`, `repeat`, `lpad`, `rpad`, `position`, `starts_with`, `ends_with`, `regexp_match`, `regexp_replace`, `split_part`, `md5`, `sha256`, and more.

#### Math Functions
`abs`, `ceil`, `floor`, `round`, `sqrt`, `cbrt`, `log`, `log2`, `log10`, `ln`, `exp`, `pow`, `sign`, `mod`, `pi`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `degrees`, `radians`, `greatest`, `least`, and more.

#### Date/Time Functions
`now`, `current_timestamp`, `dateadd`, `datediff`, `date_trunc`, `to_timestamp`, `to_str`, `extract`, `epoch`, `day_of_week`, `day_of_year`, `hour`, `minute`, `second`, `systimestamp`, and more.

#### Conditional Functions
`coalesce`, `nullif`, `greatest`, `least`, `decode`, `if`, `ifnull`

#### Random Data Generators
`rnd_int`, `rnd_long`, `rnd_double`, `rnd_float`, `rnd_str`, `rnd_symbol`, `rnd_timestamp`, `rnd_boolean`, `rnd_uuid4`, and more.

#### System Functions
`version`, `current_database`, `pg_typeof`, `generate_series`, `long_sequence`

### Access Control

```sql
-- User management
CREATE USER analyst WITH PASSWORD 'secret';
DROP USER analyst;

-- Role management
CREATE ROLE readonly;
DROP ROLE readonly;

-- Permissions
GRANT READ ON trades TO readonly;
REVOKE READ ON trades FROM readonly;
GRANT readonly TO analyst;
```

---

## Configuration

ExchangeDB loads configuration in priority order (highest wins):

1. CLI flags (`--bind`, `--data-dir`)
2. Environment variables (`EXCHANGEDB_*`)
3. Config file (`exchange-db.toml`)
4. Built-in defaults

Generate a fully commented reference config:

```bash
exchange-db config generate --output exchange-db.toml
```

A complete reference is also available at [`exchange-db.example.toml`](exchange-db.example.toml).

### Full Config Reference (`exchange-db.toml`)

```toml
[server]
data_dir = "./data"            # Root data directory for all tables
log_level = "info"             # trace, debug, info, warn, error

[http]
bind = "0.0.0.0:9000"         # HTTP REST API bind address
enabled = true                 # Enable/disable HTTP server

[pgwire]
bind = "0.0.0.0:8812"         # PostgreSQL wire protocol bind address
enabled = true                 # Enable/disable pgwire server

[ilp]
bind = "0.0.0.0:9009"         # ILP ingestion bind address
enabled = true                 # Enable/disable ILP server
batch_size = 1000              # Rows to batch before flushing

[storage]
wal_enabled = true             # Enable write-ahead log
wal_max_segment_size = "64MB"  # Max WAL segment size before rotation
default_partition_by = "day"   # Default partitioning: none, hour, day, week, month, year
mmap_page_size = "4KB"         # Memory map page size (4KB or 2MB for huge pages)

[retention]
enabled = false                # Enable automatic data retention
max_age = "30d"                # Maximum age of data before deletion
check_interval = "1h"          # How often to check for expired data

[performance]
query_parallelism = 0          # Query thread count (0 = auto, uses num_cpus)
writer_commit_mode = "async"   # WAL commit mode: "sync" or "async"

[security]
auth_enabled = false           # Enable authentication
token = ""                     # Static token for simple authentication
oauth_issuer = ""              # OAuth 2.0 / OIDC issuer URL for JWT validation
tls_cert = ""                  # Path to TLS certificate file
tls_key = ""                   # Path to TLS private key file

[replication]
mode = "standalone"            # "standalone", "primary", or "replica"
primary_addr = ""              # Address of primary server (replica mode only)
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `EXCHANGEDB_DATA_DIR` | Root data directory | `./data` |
| `EXCHANGEDB_LOG_LEVEL` | Log level (trace/debug/info/warn/error) | `info` |
| `EXCHANGEDB_HTTP_BIND` | HTTP server bind address | `0.0.0.0:9000` |
| `EXCHANGEDB_HTTP_ENABLED` | Enable HTTP server | `true` |
| `EXCHANGEDB_PGWIRE_BIND` | PostgreSQL wire protocol bind | `0.0.0.0:8812` |
| `EXCHANGEDB_PGWIRE_ENABLED` | Enable pgwire server | `true` |
| `EXCHANGEDB_ILP_BIND` | ILP ingestion bind address | `0.0.0.0:9009` |
| `EXCHANGEDB_ILP_ENABLED` | Enable ILP ingestion | `true` |
| `EXCHANGEDB_ILP_BATCH_SIZE` | ILP batch size | `1000` |
| `EXCHANGEDB_WAL_ENABLED` | Enable write-ahead log | `true` |
| `EXCHANGEDB_QUERY_PARALLELISM` | Query thread count (0 = auto) | `0` |
| `EXCHANGEDB_WRITER_COMMIT_MODE` | Writer commit mode (sync/async) | `async` |

### Hot Config Reload

On Unix systems, send `SIGHUP` to reload runtime-safe settings without restarting:

```bash
kill -HUP $(pidof exchange-db)
```

---

## Client Libraries

### Python (psycopg2)

```python
import psycopg2

conn = psycopg2.connect(host="localhost", port=8812, dbname="exchangedb")
cur = conn.cursor()

cur.execute("""
    SELECT symbol, avg(price), sum(volume)
    FROM trades
    WHERE timestamp > '2024-01-01'
    GROUP BY symbol
""")

for row in cur.fetchall():
    print(row)

conn.close()
```

### Python (HTTP via requests)

```python
import requests

resp = requests.post("http://localhost:9000/api/v1/query", json={
    "query": "SELECT symbol, last(price) FROM trades LATEST ON timestamp PARTITION BY symbol"
})

data = resp.json()
for row in data["rows"]:
    print(row)
```

### Go (pgx)

```go
package main

import (
    "context"
    "fmt"
    "github.com/jackc/pgx/v5"
)

func main() {
    conn, _ := pgx.Connect(context.Background(),
        "postgres://localhost:8812/exchangedb")
    defer conn.Close(context.Background())

    rows, _ := conn.Query(context.Background(),
        "SELECT symbol, avg(price) FROM trades GROUP BY symbol")
    defer rows.Close()

    for rows.Next() {
        var symbol string
        var avgPrice float64
        rows.Scan(&symbol, &avgPrice)
        fmt.Printf("%s: %.2f\n", symbol, avgPrice)
    }
}
```

### Node.js (pg)

```javascript
const { Client } = require('pg');

const client = new Client({
    host: 'localhost',
    port: 8812,
    database: 'exchangedb',
});

await client.connect();

const res = await client.query(`
    SELECT symbol, count(*), avg(price)
    FROM trades
    SAMPLE BY 1h
`);

console.log(res.rows);
await client.end();
```

### Rust (sqlx)

```rust
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new()
        .connect("postgres://localhost:8812/exchangedb")
        .await?;

    let rows = sqlx::query_as::<_, (String, f64)>(
        "SELECT symbol, avg(price) as avg_price FROM trades GROUP BY symbol"
    )
    .fetch_all(&pool)
    .await?;

    for (symbol, avg_price) in rows {
        println!("{symbol}: {avg_price:.2}");
    }

    Ok(())
}
```

### Java (JDBC)

```java
import java.sql.*;

public class ExchangeDBExample {
    public static void main(String[] args) throws Exception {
        Connection conn = DriverManager.getConnection(
            "jdbc:postgresql://localhost:8812/exchangedb"
        );

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            "SELECT symbol, avg(price), sum(volume) FROM trades GROUP BY symbol"
        );

        while (rs.next()) {
            System.out.printf("%s: avg=%.2f vol=%.2f%n",
                rs.getString("symbol"),
                rs.getDouble(2),
                rs.getDouble(3));
        }

        conn.close();
    }
}
```

### curl (HTTP API)

```bash
# Query with JSON response
curl -s -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM trades LIMIT 5"}' | jq

# Ingest via ILP over HTTP
curl -X POST http://localhost:9000/api/v1/write \
  -d 'trades,symbol=BTC/USD price=65000.0,volume=1.5 1710000000000000000'

# Import CSV
curl -X POST http://localhost:9000/api/v1/import \
  -F 'data=@trades.csv' \
  -F 'name=trades'

# Export CSV
curl -o trades.csv 'http://localhost:9000/api/v1/export?query=SELECT+*+FROM+trades'

# Prometheus metrics
curl http://localhost:9000/metrics

# System diagnostics
curl http://localhost:9000/api/v1/diagnostics | jq
```

---

## Architecture

ExchangeDB is organized into 6 Rust crates:

```
exchange-db/
├── crates/
│   ├── common/     Shared types (41 column types, errors, ring buffers, hashing)
│   ├── core/       Storage engine (columns, partitions, mmap, WAL, indexes, transactions)
│   ├── query/      SQL parser, planner, optimizer, executor (124 cursors, 1,198+ functions)
│   ├── net/        Network protocols (HTTP, pgwire, ILP, WebSocket, web console)
│   ├── exchange/   Exchange-specific (order book, OHLCV, tick data, market types)
│   └── server/     Binary entry point, configuration, signal handling
```

### Storage Engine (`core`)

- **Column-oriented**: Fixed columns (bool, i8-i64, f32, f64, timestamp, UUID), variable columns (string, binary), symbol columns (dictionary-encoded with int IDs)
- **Memory-mapped reads**: Zero-copy access to column files via mmap with configurable page sizes and prefetch hints
- **Time-based partitioning**: Automatic partition creation, pruning during queries, detach/attach for cold storage
- **Write-Ahead Log**: Per-table WAL with configurable sync/async commit, background merge into column store
- **Compression**: LZ4, delta encoding, RLE, dictionary encoding
- **Indexes**: Bitmap indexes on symbol columns for fast filtering

### Query Engine (`query`)

- **Cursor-based pull model** with 124 specialized cursor implementations
- **Vectorized execution** on record batches for CPU cache efficiency
- **Parallel scan** across partitions via rayon
- **Adaptive strategy selection**: automatically chooses between row-at-a-time, vectorized, columnar, or parallel execution
- **Optimizer**: partition pruning, predicate pushdown, index scan selection, limit pushdown
- **Spill-to-disk** for sorts and GROUP BY that exceed memory budget

### Data Format on Disk

```
table-name/
├── _meta              # Table metadata
├── _txn               # Transaction file (partition list, row counts)
├── _cv                # Column versions (schema evolution)
├── _wal/              # Write-ahead log segments
│   ├── wal-0/
│   │   ├── _events    # Event log
│   │   └── *.d        # Column data
│   └── wal-1/
├── 2024-03-01/        # Day partition
│   ├── timestamp.d    # Designated timestamp column
│   ├── price.d        # Fixed column (f64)
│   ├── symbol.d       # Symbol column (i32 IDs)
│   ├── symbol.k       # Symbol index keys
│   └── symbol.v       # Symbol index values
└── 2024-03-02/
```

---

## Benchmarks

All benchmarks measured on Apple Silicon (M-series), Rust 1.85, release profile, using Criterion 0.5. Full results in [BENCHMARKS.md](BENCHMARKS.md).

### Write Throughput

| Operation | Throughput | Latency |
|-----------|-----------|---------|
| Batch write (1M rows, columnar) | **18.80 M rows/s** | 53 ms |
| Batch write (1M rows, row-based) | 13.78 M rows/s | 73 ms |
| Table writer (1M rows) | 4.40 M rows/s | 227 ms |
| WAL deferred merge (100K rows) | 8.09 M rows/s | 12 ms |
| WAL sync commit (100K rows) | 990 K rows/s | 101 ms |
| ILP ingestion (SQL path, 100K rows) | 50.52 K rows/s | 1.98 s |

### Query Throughput

| Query Type | Throughput | Latency (1M rows) |
|-----------|-----------|---------|
| Multi-partition GROUP BY (30 parts) | **61.05 M rows/s** | 16 ms |
| Multi-partition scan (30 parts) | 17.30 M rows/s | 58 ms |
| Parallel scan (30 parts) | 13.08 M rows/s | 76 ms |
| GROUP BY (100 groups) | 11.64 M rows/s | 86 ms |
| SAMPLE BY 1h | 6.68 M rows/s | 150 ms |
| Full scan (SELECT *) | 6.15 M rows/s | 163 ms |
| Filtered scan (price > 50000) | 5.05 M rows/s | 198 ms |
| LATEST ON (100 symbols) | 4.07 M rows/s | 246 ms |

### Storage Engine

| Operation | Throughput |
|-----------|-----------|
| Column read (f64, 1M) | 590.70 M elements/s |
| SIMD sum (f64, 1M) | 4.49 G elements/s |
| Delta encode timestamps (1M) | 5.59 G elements/s |
| LZ4 compress (f64, 1M) | 18.22 GiB/s |
| Bitmap index lookup | 767.50 M lookups/s |
| Partition pruning (1-of-30) | 45.5 us |

### Exchange-Specific

| Operation | Throughput |
|-----------|-----------|
| OHLCV aggregation (1M ticks, 1s bars) | 163.64 M ticks/s |
| Order book delta apply (100K) | 91.44 M deltas/s |
| Delta encode prices (1M) | 731.04 M elements/s |

### TSBS DevOps (1M rows, 100 hosts)

| Query | Throughput |
|-------|-----------|
| Max CPU 12h | 1.19 G rows/s |
| GROUP BY + ORDER BY + LIMIT 5 | 41.25 M rows/s |
| High CPU (filtered, top 10) | 15.22 M rows/s |
| Aggregate all | 13.66 M rows/s |
| Double GROUP BY (SAMPLE BY 1h) | 9.50 M rows/s |
| Last point per host | 7.15 M rows/s |

---

## Documentation

Full documentation is in the [`docs/`](docs/) directory:

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/GETTING_STARTED.md) | Install to first query in 5 minutes |
| [User Guide](docs/USER_GUIDE.md) | Comprehensive walkthrough |
| [SQL Reference](docs/SQL_REFERENCE.md) | Complete SQL dialect reference |
| [Functions Reference](docs/FUNCTIONS_REFERENCE.md) | All 1,198+ functions |
| [API Reference](docs/API_REFERENCE.md) | HTTP, pgwire, ILP, WebSocket |
| [CLI Reference](docs/CLI_REFERENCE.md) | All CLI commands |
| [Configuration](docs/CONFIGURATION_COMPLETE.md) | All config options |
| [Operations](docs/OPERATIONS.md) | Backup, monitoring, retention |
| [Replication](docs/REPLICATION.md) | Primary-replica setup |
| [Security](docs/SECURITY.md) | Auth, encryption, audit |
| [Ingestion](docs/INGESTION.md) | High-throughput data loading |
| [Data Types](docs/DATA_TYPES.md) | All 41 column types |
| [Migration from QuestDB](docs/MIGRATION_FROM_QUESTDB.md) | Side-by-side comparison |
| [Troubleshooting](docs/TROUBLESHOOTING.md) | Common issues and fixes |

---

## Contributing

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Check compilation without building
cargo check
```

### Running Tests

```bash
# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p exchange-core
cargo test -p exchange-query
cargo test -p exchange-net

# Run a specific test
cargo test -p exchange-query test_sample_by
```

### Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run benchmarks for a specific crate
cargo bench -p exchange-core
cargo bench -p exchange-query

# Run a specific benchmark
cargo bench -p exchange-core -- column_write
```

### Project Structure

| Crate | Description |
|-------|-------------|
| `exchange-common` | Shared types: 41 column types, error types, ring buffers, hashing |
| `exchange-core` | Storage engine: columns, partitions, mmap, WAL, indexes, transactions |
| `exchange-query` | Query engine: SQL parser, planner, optimizer, 124 cursors, 1,198+ functions |
| `exchange-net` | Network: HTTP, PostgreSQL wire protocol, ILP, WebSocket, web console |
| `exchange-exchange` | Exchange domain: order book, OHLCV, tick data, market types |
| `exchange-server` | Server binary: CLI, configuration, signal handling, log rotation |

### Code Stats

| Metric | Value |
|--------|-------|
| Lines of Rust | ~200,000 |
| Source files | ~361 |
| Crates | 6 |
| Tests | ~26,500 passing |
| SQL functions | 1,198+ |
| Aggregate kinds | 120+ |
| Cursor strategies | 124 |
| Data types | 41 |
| Insert throughput | 8-16M rows/s |
| Full scan throughput | 17.4M rows/s |

---

## License

ExchangeDB is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

Copyright 2026 Dmytro Chystiakov.
