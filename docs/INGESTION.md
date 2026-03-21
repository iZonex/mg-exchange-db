# ExchangeDB Ingestion Guide

This guide covers all methods for getting data into ExchangeDB: InfluxDB Line
Protocol (ILP), SQL INSERT, CSV import, and bulk loading strategies.

---

## Table of Contents

1. [Overview](#overview)
2. [ILP over TCP](#ilp-over-tcp)
3. [ILP over HTTP](#ilp-over-http)
4. [ILP over UDP](#ilp-over-udp)
5. [SQL INSERT](#sql-insert)
6. [CSV Import (CLI)](#csv-import-cli)
7. [CSV Import (HTTP)](#csv-import-http)
8. [CSV Import (SQL COPY)](#csv-import-sql-copy)
9. [Auto Table Creation](#auto-table-creation)
10. [Bulk Loading Best Practices](#bulk-loading-best-practices)
11. [Out-of-Order Data](#out-of-order-data)
12. [Deduplication](#deduplication)
13. [Performance Tuning](#performance-tuning)

---

## Overview

ExchangeDB supports multiple ingestion paths, each optimized for different
use cases:

| Method | Port | Throughput | Use Case |
|--------|------|-----------|----------|
| ILP/TCP | 9009 | Highest (~50K+ rows/s per conn) | Real-time streaming, market data feeds |
| ILP/HTTP | 9000 | High | Batch ingestion, cloud environments |
| ILP/UDP | 9009 | Medium | Fire-and-forget, metrics |
| SQL INSERT | 8812/9000 | Medium | Ad-hoc inserts, application code |
| CSV Import (CLI) | N/A | High (bulk) | Historical data loading |
| CSV Import (HTTP) | 9000 | High (bulk) | Programmatic bulk loading |
| COPY FROM | 8812 | High (bulk) | PostgreSQL-compatible bulk loading |

The internal write path achieves up to 18.8M rows/s for batch columnar writes
and 8.09M rows/s for WAL-deferred merges.

---

## ILP over TCP

InfluxDB Line Protocol over TCP is the recommended method for real-time,
high-throughput ingestion.

### Connection

Connect to port 9009 (default) via TCP:

```bash
nc localhost 9009
```

### Line Format

```
measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 [timestamp_ns]
```

| Component | Description | Required |
|-----------|-------------|----------|
| `measurement` | Table name | Yes |
| Tags | Key=value pairs, comma-separated, after measurement name | No |
| Fields | Key=value pairs, space-separated from tags | Yes (at least one) |
| Timestamp | Nanoseconds since Unix epoch | No (defaults to `now()`) |

### Field Types

| Suffix | Type | Example |
|--------|------|---------|
| (none / decimal) | Double (F64) | `price=65000.0` |
| `i` | Long (I64) | `count=42i` |
| `"..."` | String (Varchar) | `message="filled"` |
| `true`/`false` | Boolean | `active=true` |

### Tag Behavior

- Tags are stored as **SYMBOL** columns (dictionary-encoded, auto-indexed).
- Tags are ideal for low-cardinality values like ticker symbols, exchange
  names, and side indicators.

### Examples

**Single line:**

```bash
echo 'trades,symbol=BTC/USD,side=buy price=65000.0,volume=1.5 1709283600000000000' \
  | nc localhost 9009
```

**Multiple lines:**

```bash
echo 'trades,symbol=BTC/USD price=65000.0,volume=1.5 1709283600000000000
trades,symbol=ETH/USD price=3400.0,volume=10.0 1709283601000000000
trades,symbol=SOL/USD price=120.5,volume=50.0 1709283602000000000' \
  | nc localhost 9009
```

**From a file:**

```bash
cat market_data.ilp | nc localhost 9009
```

**Continuous streaming (Python):**

```python
import socket
import time

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 9009))

while True:
    ts = int(time.time() * 1e9)
    line = f'trades,symbol=BTC/USD price=65000.0,volume=1.5 {ts}\n'
    sock.sendall(line.encode())
    time.sleep(0.001)  # 1000 rows/sec

sock.close()
```

### Batching

ExchangeDB batches incoming ILP lines before flushing to the WAL. The batch
size is configurable:

```toml
[ilp]
batch_size = 1000  # Rows to batch before flushing
```

For maximum throughput, send data in batches rather than one line at a time.
Buffer multiple lines and send them together:

```python
buffer = []
for trade in trades:
    buffer.append(f'trades,symbol={trade.symbol} price={trade.price},volume={trade.volume} {trade.ts}\n')
    if len(buffer) >= 1000:
        sock.sendall(''.join(buffer).encode())
        buffer.clear()
```

### Authentication

When `security.auth_enabled` is `true`, ILP connections must authenticate.
The authentication token is sent as the first line of the TCP connection.

---

## ILP over HTTP

ILP ingestion via HTTP POST is useful when TCP connections are not practical
(e.g., behind load balancers, in serverless environments).

### Endpoint

```
POST http://localhost:9000/api/v1/write
```

### Examples

**Single line:**

```bash
curl -X POST http://localhost:9000/api/v1/write \
  -d 'trades,symbol=BTC/USD price=65000.0,volume=1.5 1709283600000000000'
```

**Multiple lines:**

```bash
curl -X POST http://localhost:9000/api/v1/write \
  -d 'trades,symbol=BTC/USD price=65000.0,volume=1.5 1709283600000000000
trades,symbol=ETH/USD price=3400.0,volume=10.0 1709283601000000000'
```

**From file:**

```bash
curl -X POST http://localhost:9000/api/v1/write \
  --data-binary @market_data.ilp
```

### Response

```json
{
  "status": "ok",
  "lines_accepted": 2
}
```

---

## ILP over UDP

ILP over UDP is fire-and-forget: no acknowledgment, no backpressure. Suitable
for non-critical metrics where occasional data loss is acceptable.

### Sending

```bash
echo 'metrics,host=server1 cpu=0.85,mem=0.72' | nc -u localhost 9009
```

### Limitations

- No delivery guarantee (UDP packets may be dropped).
- No error reporting.
- Maximum line size limited by UDP packet size (~65KB).
- Not recommended for financial trade data.

---

## SQL INSERT

### Via psql / PostgreSQL Wire Protocol

```sql
-- Single row
INSERT INTO trades VALUES ('2024-03-01T10:00:00Z', 'BTC/USD', 65000.0, 1.5, 'buy');

-- Multiple rows
INSERT INTO trades VALUES
    ('2024-03-01T10:00:00Z', 'BTC/USD', 65000.0, 1.5, 'buy'),
    ('2024-03-01T10:00:01Z', 'ETH/USD', 3400.0, 10.0, 'sell'),
    ('2024-03-01T10:00:02Z', 'SOL/USD', 120.5, 50.0, 'buy');

-- Named columns
INSERT INTO trades (timestamp, symbol, price)
VALUES ('2024-03-01T10:00:00Z', 'BTC/USD', 65000.0);

-- INSERT from SELECT (bulk copy between tables)
INSERT INTO trades_archive
SELECT * FROM trades WHERE timestamp < '2024-01-01';
```

### Via HTTP

```bash
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "INSERT INTO trades VALUES (now(), '\''BTC/USD'\'', 65000.0, 1.5, '\''buy'\'')"}'
```

### Upsert (INSERT ON CONFLICT)

```sql
INSERT INTO trades VALUES ('2024-03-01T10:00:00Z', 'BTC/USD', 65100.0, 1.5, 'buy')
ON CONFLICT DO UPDATE SET price = EXCLUDED.price;
```

---

## CSV Import (CLI)

The `exchange-db import` command loads CSV files directly:

```bash
# Basic import
exchange-db import --table trades --file trades.csv

# Custom delimiter
exchange-db import --table trades --file trades.tsv --delimiter '\t'
```

The table must already exist, or will be auto-created with schema inferred
from the CSV header.

### CSV Format Requirements

- First row must be a header with column names.
- Column names must match the table schema.
- Timestamps should be in ISO 8601 format or nanoseconds since epoch.
- Values separated by commas (default) or a custom delimiter.

### Example CSV

```csv
timestamp,symbol,price,volume,side
2024-03-01T10:00:00.000000000Z,BTC/USD,65000.0,1.5,buy
2024-03-01T10:00:01.000000000Z,ETH/USD,3400.0,10.0,sell
2024-03-01T10:00:02.000000000Z,SOL/USD,120.5,50.0,buy
```

---

## CSV Import (HTTP)

### Endpoint

```
POST http://localhost:9000/api/v1/import?table=<table_name>
```

### Examples

**From file:**

```bash
curl -X POST 'http://localhost:9000/api/v1/import?table=trades' \
  -H 'Content-Type: text/csv' \
  --data-binary @trades.csv
```

**Inline data:**

```bash
curl -X POST 'http://localhost:9000/api/v1/import?table=trades' \
  -H 'Content-Type: text/csv' \
  -d 'timestamp,symbol,price,volume
1709283600000000000,BTC/USD,65000.0,1.5
1709283601000000000,ETH/USD,3400.0,10.0'
```

### Response

```json
{
  "rows_imported": 2,
  "table": "trades"
}
```

---

## CSV Import (SQL COPY)

Use the SQL `COPY FROM` statement for PostgreSQL-compatible bulk loading:

```sql
-- Import CSV
COPY trades FROM '/path/to/trades.csv' WITH (FORMAT CSV, HEADER TRUE);

-- Import TSV
COPY trades FROM '/path/to/trades.tsv' WITH (FORMAT TSV, HEADER TRUE);

-- Import Parquet
COPY trades FROM '/path/to/trades.parquet' WITH (FORMAT PARQUET);
```

The `COPY` protocol is also supported via the pgwire extended query protocol
for programmatic bulk loading from PostgreSQL client libraries.

---

## Auto Table Creation

ExchangeDB automatically creates tables when data arrives via ILP and the
target table does not exist.

### How It Works

1. The first ILP line for a new measurement name triggers table creation.
2. The schema is inferred from the line's tags and fields:
   - Tags become `SYMBOL` columns.
   - Float fields become `DOUBLE` columns.
   - Integer fields (with `i` suffix) become `LONG` columns.
   - String fields become `VARCHAR` columns.
   - Boolean fields become `BOOLEAN` columns.
3. A `timestamp` column of type `TIMESTAMP` is always added.
4. The table is created with `PARTITION BY DAY` by default (configurable via
   `storage.default_partition_by`).

### Example

Sending this ILP line:

```
trades,symbol=BTC/USD,exchange=binance price=65000.0,volume=1.5,count=42i 1709283600000000000
```

Automatically creates a table equivalent to:

```sql
CREATE TABLE trades (
    timestamp TIMESTAMP,
    symbol    SYMBOL,
    exchange  SYMBOL,
    price     DOUBLE,
    volume    DOUBLE,
    count     LONG
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

### Disabling Auto-Creation

Auto table creation can be controlled via the ILP configuration. When disabled,
ILP lines for non-existent tables are rejected.

---

## Bulk Loading Best Practices

### 1. Use ILP for Streaming Data

For real-time market data feeds, use ILP/TCP with batching:

- Buffer 1,000-10,000 rows before sending.
- Use multiple TCP connections for parallel ingestion.
- Keep connections persistent (avoid connect/disconnect overhead).

### 2. Use CSV/COPY for Historical Loads

For loading large historical datasets:

```bash
# Split large files for parallel loading
split -l 1000000 large_data.csv chunk_
for f in chunk_*; do
    exchange-db import --table trades --file "$f" &
done
wait
```

### 3. Pre-Sort Data by Timestamp

ExchangeDB handles out-of-order data, but sorted data is faster to ingest
because it avoids the O3 (out-of-order) merge path:

```bash
sort -t, -k1 unsorted.csv > sorted.csv
exchange-db import --table trades --file sorted.csv
```

### 4. Choose the Right Partition Size

| Data Volume | Recommended Partition |
|-------------|----------------------|
| < 1M rows/day | `MONTH` or `YEAR` |
| 1M-100M rows/day | `DAY` |
| 100M+ rows/day | `HOUR` |
| Sub-second queries | `HOUR` (more partition pruning) |

### 5. Use SYMBOL for Low-Cardinality Strings

Use `SYMBOL` type instead of `VARCHAR` for columns with < ~100K unique
values (ticker symbols, exchange names, side indicators). SYMBOL columns
are dictionary-encoded and auto-indexed, providing much faster filtering.

### 6. Batch INSERT Statements

Instead of individual INSERT statements, use multi-row INSERT:

```sql
-- Slow: one round-trip per row
INSERT INTO trades VALUES (...);
INSERT INTO trades VALUES (...);
INSERT INTO trades VALUES (...);

-- Fast: one round-trip for all rows
INSERT INTO trades VALUES
    (...),
    (...),
    (...);
```

### 7. Use Async WAL Commit for Throughput

For maximum write throughput when durability can tolerate a small window:

```toml
[performance]
writer_commit_mode = "async"
```

For maximum durability:

```toml
[performance]
writer_commit_mode = "sync"
```

---

## Out-of-Order Data

ExchangeDB supports out-of-order (O3) ingestion. Rows with timestamps
older than the current maximum are handled through the O3 merge path.

### How O3 Works

1. In-order rows are appended directly to the active partition.
2. Out-of-order rows are written to the WAL.
3. A background merge job integrates O3 rows into the correct partitions.
4. Queries always see a consistent view including uncommitted O3 rows.

### Performance Impact

O3 ingestion is slower than in-order because it requires:

- Locating the correct partition.
- Merging new rows with existing sorted data.
- Potential partition rewrite.

For best performance, pre-sort data by timestamp before loading.

---

## Deduplication

ExchangeDB supports row deduplication on insert:

```sql
-- Upsert: update if exists, insert if new
INSERT INTO trades VALUES (...)
ON CONFLICT DO UPDATE SET price = EXCLUDED.price;

-- Skip duplicates
INSERT INTO trades VALUES (...)
ON CONFLICT DO NOTHING;
```

The dedup engine can also be configured to automatically deduplicate rows
with identical timestamps and symbol values during WAL merge.

---

## Performance Tuning

### ILP Batch Size

Increase the ILP batch size for higher throughput:

```toml
[ilp]
batch_size = 10000  # Default: 1000
```

### WAL Segment Size

Larger WAL segments reduce the frequency of segment rotation:

```toml
[storage]
wal_max_segment_size = "256MB"  # Default: 64MB
```

### Query Parallelism

More query threads allow faster concurrent ingestion and querying:

```toml
[performance]
query_parallelism = 8  # Default: 0 (auto = num_cpus)
```

### Memory-Mapped Page Size

Use huge pages for large datasets:

```toml
[storage]
mmap_page_size = "2MB"  # Default: 4KB
```

Requires huge pages enabled at the OS level:

```bash
# Linux
echo 1024 | sudo tee /proc/sys/vm/nr_hugepages
```
