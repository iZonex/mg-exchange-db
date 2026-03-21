# Migrating from QuestDB to ExchangeDB

This guide covers how to migrate from QuestDB to ExchangeDB. ExchangeDB is designed to be largely compatible with QuestDB's SQL dialect and protocols, but there are important differences to be aware of.

---

## Table of Contents

1. [Compatibility Overview](#compatibility-overview)
2. [Schema Compatibility](#schema-compatibility)
3. [SQL Differences](#sql-differences)
4. [ILP Compatibility](#ilp-compatibility)
5. [PostgreSQL Wire Protocol](#postgresql-wire-protocol)
6. [HTTP API Differences](#http-api-differences)
7. [Configuration Mapping](#configuration-mapping)
8. [Data Migration Steps](#data-migration-steps)
9. [Client Library Changes](#client-library-changes)
10. [Feature Comparison](#feature-comparison)

---

## Compatibility Overview

| Feature | QuestDB | ExchangeDB | Compatible? |
|---------|---------|------------|-------------|
| SQL dialect | QuestDB SQL | QuestDB-compatible SQL | Yes (with minor differences) |
| ILP ingestion (TCP) | Port 9009 | Port 9009 | Yes |
| ILP ingestion (HTTP) | `/api/v1/write` | `/api/v1/write` | Yes |
| PostgreSQL wire protocol | Port 8812 | Port 8812 | Yes |
| REST API | `/exec?query=` | `/api/v1/query` | Different URL, same concept |
| `SAMPLE BY` | Yes | Yes | Yes |
| `LATEST ON` | Yes | Yes | Yes |
| `ASOF JOIN` | Yes | Yes | Yes |
| `FILL` | Yes | Yes | Yes |
| Partitioning | `PARTITION BY` | `PARTITION BY` | Yes |
| Designated timestamp | `TIMESTAMP()` | `TIMESTAMP()` | Yes |
| `SYMBOL` type | Yes | Yes | Yes |
| WAL mode | Yes | Yes | Yes |
| Web console | Port 9000 | Port 9000 (`/`) | Yes |
| Prometheus metrics | `/metrics` | `/metrics` | Yes |

---

## Schema Compatibility

### CREATE TABLE

QuestDB and ExchangeDB use the same `CREATE TABLE` syntax:

```sql
-- Works identically in both databases
CREATE TABLE trades (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    price DOUBLE,
    volume DOUBLE
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

### Data Types

| QuestDB Type | ExchangeDB Type | Notes |
|-------------|-----------------|-------|
| `BOOLEAN` | `BOOLEAN` | Identical |
| `BYTE` | `TINYINT` / `I8` | Same storage, different name |
| `SHORT` | `SMALLINT` / `I16` | Same storage, different name |
| `INT` | `INT` / `I32` | Identical |
| `LONG` | `BIGINT` / `I64` | Same storage, different name |
| `FLOAT` | `FLOAT` / `F32` | Identical |
| `DOUBLE` | `DOUBLE` / `F64` | Identical |
| `TIMESTAMP` | `TIMESTAMP` | Identical (nanosecond precision) |
| `SYMBOL` | `SYMBOL` | Identical (interned strings) |
| `STRING` | `VARCHAR` | Same concept, different name |
| `CHAR` | `CHAR` | Identical |
| `UUID` | `UUID` | Identical |
| `LONG256` | `LONG256` | Identical |
| `GEOHASH` | `GEOHASH` | Identical |
| `DATE` | `DATE` | Identical |
| `BINARY` | `BINARY` | Identical |
| `IPV4` | `IPV4` | Identical |

**Migration note:** If your DDL uses `STRING`, change it to `VARCHAR`. If it uses `LONG`, change it to `BIGINT` or `I64`. The `BYTE` type should be changed to `TINYINT` and `SHORT` to `SMALLINT`.

### Partition Strategies

Both databases support the same partition strategies:

| Strategy | QuestDB | ExchangeDB |
|----------|---------|------------|
| None | `PARTITION BY NONE` | `PARTITION BY NONE` |
| Hour | `PARTITION BY HOUR` | `PARTITION BY HOUR` |
| Day | `PARTITION BY DAY` | `PARTITION BY DAY` |
| Week | `PARTITION BY WEEK` | `PARTITION BY WEEK` |
| Month | `PARTITION BY MONTH` | `PARTITION BY MONTH` |
| Year | `PARTITION BY YEAR` | `PARTITION BY YEAR` |

---

## SQL Differences

### Fully Compatible Statements

These work identically in both databases:

```sql
-- SAMPLE BY with FILL
SELECT symbol, avg(price), sum(volume)
FROM trades
WHERE timestamp > '2024-01-01'
SAMPLE BY 1h
FILL(PREV);

-- LATEST ON
SELECT * FROM trades
LATEST ON timestamp PARTITION BY symbol;

-- ASOF JOIN
SELECT t.*, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q ON (t.symbol = q.symbol);

-- Time functions
SELECT dateadd('h', -24, now());

-- INSERT
INSERT INTO trades VALUES (now(), 'BTC/USD', 65000.0, 1.5);
```

### REST API Endpoint Changes

The most significant difference is the HTTP REST API URL:

| Operation | QuestDB | ExchangeDB |
|-----------|---------|------------|
| Execute SQL | `GET /exec?query=SELECT...` | `POST /api/v1/query` with JSON body |
| ILP write | `POST /write` | `POST /api/v1/write` |
| Health check | `GET /` (web console) | `GET /api/v1/health` |
| CSV export | `GET /exp?query=SELECT...` | `GET /api/v1/export?query=SELECT...` |
| CSV import | `POST /imp` | `POST /api/v1/import?table=name` |

**QuestDB:**

```bash
curl -G 'http://localhost:9000/exec' --data-urlencode 'query=SELECT * FROM trades LIMIT 10'
```

**ExchangeDB:**

```bash
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM trades LIMIT 10"}'
```

### Response Format Differences

**QuestDB response:**

```json
{
  "query": "SELECT * FROM trades LIMIT 1",
  "columns": [
    {"name": "timestamp", "type": "TIMESTAMP"},
    {"name": "symbol", "type": "SYMBOL"},
    {"name": "price", "type": "DOUBLE"}
  ],
  "dataset": [
    ["2024-03-15T10:00:00.000000Z", "BTC/USD", 65000.0]
  ],
  "count": 1,
  "timings": {
    "compiler": 1234,
    "execute": 5678,
    "count": 100
  }
}
```

**ExchangeDB response:**

```json
{
  "columns": [
    {"name": "timestamp", "type": "Varchar"},
    {"name": "symbol", "type": "Varchar"},
    {"name": "price", "type": "Varchar"}
  ],
  "rows": [
    [1710500400000000000, "BTC/USD", 65000.0]
  ],
  "timing_ms": 1.23
}
```

Key differences:
- `dataset` is renamed to `rows`
- `count` is not a separate field (use `rows.length`)
- `timings` is simplified to `timing_ms` (single float, milliseconds)
- Timestamps are returned as nanosecond integers, not ISO strings
- The `query` field is not echoed in the response

### SQL Syntax Differences

| Feature | QuestDB | ExchangeDB | Notes |
|---------|---------|------------|-------|
| `ALTER TABLE ADD COLUMN` | `ALTER TABLE t ADD COLUMN c TYPE` | Same | Compatible |
| `ALTER TABLE DROP COLUMN` | `ALTER TABLE t DROP COLUMN c` | Same | Compatible |
| `ALTER TABLE RENAME COLUMN` | `ALTER TABLE t RENAME COLUMN a TO b` | Same | Compatible |
| `COPY` | `COPY table FROM 'path'` | Use `/api/v1/import` or CLI `import` | Different mechanism |
| `VACUUM TABLE` | `VACUUM TABLE t` | `POST /admin/vacuum/t` | Admin API |
| `SNAPSHOT` | `SNAPSHOT PREPARE` / `COMPLETE` | CLI `exchange-db snapshot` | CLI command |
| `UPDATE` | Limited support | Via SQL | Check current support |
| System tables | `tables()`, `table_columns('t')` | `GET /api/v1/tables`, `GET /api/v1/tables/t` | API instead of functions |

---

## ILP Compatibility

The InfluxDB Line Protocol implementation is fully compatible. Both databases use the same format on the same default port (9009).

**No changes needed for ILP ingestion:**

```bash
# Works identically for both QuestDB and ExchangeDB
echo 'trades,symbol=BTC/USD price=65000.0,volume=1.5' | nc localhost 9009
```

### ILP over HTTP

The HTTP write endpoint uses the same ILP format but a different URL:

**QuestDB:**

```bash
curl -X POST http://localhost:9000/write \
  -d 'trades,symbol=BTC/USD price=65000.0,volume=1.5'
```

**ExchangeDB:**

```bash
curl -X POST http://localhost:9000/api/v1/write \
  -d 'trades,symbol=BTC/USD price=65000.0,volume=1.5'
```

### ILP Client Libraries

If you use the QuestDB ILP client libraries, you need to update the HTTP endpoint URL. TCP connections (port 9009) work without changes.

For the official `questdb-rs` Rust client or `questdb` Python client:
- TCP sender: no changes needed (same protocol, same port)
- HTTP sender: change the base URL from `/write` to `/api/v1/write`

---

## PostgreSQL Wire Protocol

Both databases implement pgwire on the same default port (8812). Existing `psql` connections and PostgreSQL client library code work without changes.

```bash
# Works identically for both
psql -h localhost -p 8812 -d exchangedb
```

**No changes needed** for:
- `psycopg2` (Python)
- `pg` (Node.js)
- `lib/pq` (Go)
- JDBC PostgreSQL driver (Java)
- Any other PostgreSQL client library

---

## HTTP API Differences

### Adapter Pattern

If you have existing code using the QuestDB HTTP API, here is how to adapt each endpoint:

#### Query Execution

**QuestDB:**
```python
import requests
resp = requests.get("http://localhost:9000/exec", params={"query": "SELECT * FROM trades"})
data = resp.json()
for row in data["dataset"]:
    print(row)
```

**ExchangeDB:**
```python
import requests
resp = requests.post("http://localhost:9000/api/v1/query", json={"query": "SELECT * FROM trades"})
data = resp.json()
for row in data["rows"]:
    print(row)
```

#### CSV Export

**QuestDB:**
```bash
curl 'http://localhost:9000/exp?query=SELECT+*+FROM+trades'
```

**ExchangeDB:**
```bash
curl 'http://localhost:9000/api/v1/export?query=SELECT+*+FROM+trades&format=csv'
```

#### CSV Import

**QuestDB:**
```bash
curl -F data=@trades.csv 'http://localhost:9000/imp?name=trades'
```

**ExchangeDB:**
```bash
curl -X POST 'http://localhost:9000/api/v1/import?table=trades' \
  -d @trades.csv
```

---

## Configuration Mapping

### Server Properties to TOML

QuestDB uses Java-style property files. ExchangeDB uses TOML.

| QuestDB Property (`server.conf`) | ExchangeDB Config (`exchange-db.toml`) | Environment Variable |
|----------------------------------|----------------------------------------|---------------------|
| `http.bind.to=0.0.0.0:9000` | `http.bind = "0.0.0.0:9000"` | `EXCHANGEDB_HTTP_BIND` |
| `http.enabled=true` | `http.enabled = true` | `EXCHANGEDB_HTTP_ENABLED` |
| `pg.net.bind.to=0.0.0.0:8812` | `pgwire.bind = "0.0.0.0:8812"` | `EXCHANGEDB_PGWIRE_BIND` |
| `pg.enabled=true` | `pgwire.enabled = true` | `EXCHANGEDB_PGWIRE_ENABLED` |
| `line.tcp.net.bind.to=0.0.0.0:9009` | `ilp.bind = "0.0.0.0:9009"` | `EXCHANGEDB_ILP_BIND` |
| `line.tcp.enabled=true` | `ilp.enabled = true` | `EXCHANGEDB_ILP_ENABLED` |
| `cairo.sql.backup.dir.path=/backups` | `backup.destination = "/backups"` | `EXCHANGEDB_BACKUP_DESTINATION` |
| `cairo.max.uncommitted.rows=500000` | `cairo.max_uncommitted_rows = 500000` | `EXCHANGEDB_CAIRO_MAX_UNCOMMITTED_ROWS` |
| `cairo.commit.lag=10000000` | `cairo.commit_lag = "10s"` | `EXCHANGEDB_CAIRO_COMMIT_LAG` |
| `cairo.o3.max.lag=600000000` | `cairo.o3_max_lag = "600s"` | `EXCHANGEDB_CAIRO_O3_MAX_LAG` |
| `cairo.writer.data.append.page.size=16M` | `cairo.writer_data_append_page_size = "16MB"` | `EXCHANGEDB_CAIRO_WRITER_DATA_APPEND_PAGE_SIZE` |
| `cairo.default.map.type=fast` | `cairo.default_map_type = "fast"` | `EXCHANGEDB_CAIRO_DEFAULT_MAP_TYPE` |
| `cairo.default.symbol.cache.flag=true` | `cairo.default_symbol_cache_flag = true` | `EXCHANGEDB_CAIRO_DEFAULT_SYMBOL_CACHE_FLAG` |
| `cairo.default.symbol.capacity=256` | `cairo.default_symbol_capacity = 256` | `EXCHANGEDB_CAIRO_DEFAULT_SYMBOL_CAPACITY` |
| `cairo.spin.lock.timeout=5000` | `cairo.spin_lock_timeout = "5s"` | `EXCHANGEDB_CAIRO_SPIN_LOCK_TIMEOUT` |
| `cairo.wal.enabled.default=true` | `wal.enabled = true` | `EXCHANGEDB_WAL_ENABLED` |
| `cairo.wal.max.segment.size=67108864` | `wal.max_segment_size = "64MB"` | `EXCHANGEDB_WAL_MAX_SEGMENT_SIZE` |
| `cairo.wal.purge.interval=30000` | `wal.purge_interval = "30s"` | `EXCHANGEDB_WAL_PURGE_INTERVAL` |
| `cairo.wal.segment.rollover.row.count=200000` | `wal.segment_rollover_row_count = 200000` | `EXCHANGEDB_WAL_SEGMENT_ROLLOVER_ROW_COUNT` |
| `cairo.wal.squash.uncommitted.rows.multiplier=20.0` | `wal.squash_uncommitted_rows_multiplier = 20.0` | `EXCHANGEDB_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER` |
| `cairo.o3.partition.split.min.size=50M` | `o3.partition_split_min_size = "50MB"` | `EXCHANGEDB_O3_PARTITION_SPLIT_MIN_SIZE` |
| `cairo.o3.last.partition.max.splits=20` | `o3.last_partition_max_splits = 20` | `EXCHANGEDB_O3_LAST_PARTITION_MAX_SPLITS` |
| `cairo.o3.column.memory.size=8M` | `o3.column_memory_size = "8MB"` | `EXCHANGEDB_O3_COLUMN_MEMORY_SIZE` |
| `telemetry.enabled=true` | `telemetry.enabled = true` | `EXCHANGEDB_TELEMETRY_ENABLED` |
| `query.timeout.sec=60` | `memory.max_per_query = "256MB"` | `EXCHANGEDB_MEMORY_MAX_PER_QUERY` |

### Key Differences in Configuration

1. **Duration format:** QuestDB uses milliseconds or microseconds as raw integers. ExchangeDB uses human-readable strings like `"10s"`, `"30d"`, `"1h"`.

2. **Size format:** QuestDB uses raw byte counts or Java-style sizes. ExchangeDB uses human-readable strings like `"64MB"`, `"4KB"`.

3. **File format:** QuestDB uses Java properties files (`.conf`). ExchangeDB uses TOML (`.toml`).

4. **Environment variables:** QuestDB uses `QDB_` prefix. ExchangeDB uses `EXCHANGEDB_` prefix.

---

## Data Migration Steps

### Method 1: CSV Export/Import (Recommended for Small to Medium Datasets)

This is the simplest approach. Export data from QuestDB as CSV and import into ExchangeDB.

**Step 1: Export from QuestDB**

```bash
# Export each table as CSV
curl -G 'http://questdb-host:9000/exp' \
  --data-urlencode 'query=SELECT * FROM trades' \
  -o trades.csv

curl -G 'http://questdb-host:9000/exp' \
  --data-urlencode 'query=SELECT * FROM quotes' \
  -o quotes.csv
```

**Step 2: Create tables in ExchangeDB**

```bash
# Recreate the schema (adjust types as needed)
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "CREATE TABLE trades (timestamp TIMESTAMP, symbol SYMBOL, price DOUBLE, volume DOUBLE) TIMESTAMP(timestamp) PARTITION BY DAY"
  }'
```

**Step 3: Import into ExchangeDB**

Using the CLI:

```bash
exchange-db import --data-dir ./data --table trades --file trades.csv
exchange-db import --data-dir ./data --table quotes --file quotes.csv
```

Or using the HTTP API:

```bash
curl -X POST 'http://localhost:9000/api/v1/import?table=trades' \
  --data-binary @trades.csv
```

### Method 2: ILP Replay (Recommended for Large Datasets)

For large datasets, export from QuestDB and replay via ILP for higher throughput.

**Step 1: Export from QuestDB as ordered data**

```bash
curl -G 'http://questdb-host:9000/exp' \
  --data-urlencode 'query=SELECT * FROM trades ORDER BY timestamp' \
  -o trades.csv
```

**Step 2: Convert CSV to ILP and stream to ExchangeDB**

```python
import csv
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost", 9009))

with open("trades.csv") as f:
    reader = csv.DictReader(f)
    batch = []
    for row in reader:
        line = f"trades,symbol={row['symbol']} price={row['price']},volume={row['volume']} {row['timestamp']}\n"
        batch.append(line)

        if len(batch) >= 10000:
            sock.sendall("".join(batch).encode())
            batch = []

    if batch:
        sock.sendall("".join(batch).encode())

sock.close()
```

### Method 3: pgwire SQL Dump

Since both databases support pgwire, you can use `pg_dump`-style tools or script a migration:

```python
import psycopg2

# Connect to QuestDB
src = psycopg2.connect(host="questdb-host", port=8812, dbname="qdb")
src_cur = src.cursor()

# Connect to ExchangeDB
dst = psycopg2.connect(host="localhost", port=8812, dbname="exchangedb")
dst_cur = dst.cursor()

# Create table in ExchangeDB
dst_cur.execute("""
    CREATE TABLE trades (
        timestamp TIMESTAMP,
        symbol VARCHAR,
        price DOUBLE,
        volume DOUBLE
    ) TIMESTAMP(timestamp) PARTITION BY DAY
""")

# Copy data in batches
src_cur.execute("SELECT * FROM trades ORDER BY timestamp")
batch_size = 10000
while True:
    rows = src_cur.fetchmany(batch_size)
    if not rows:
        break
    for row in rows:
        dst_cur.execute(
            "INSERT INTO trades VALUES (%s, %s, %s, %s)",
            row
        )

src.close()
dst.close()
```

### Method 4: Filesystem Copy (Advanced)

ExchangeDB's storage layout is inspired by QuestDB's. For tables with compatible schemas, you may be able to copy the data files directly. This is **not recommended** for production use since the on-disk format may differ in subtle ways. Use CSV or ILP migration instead.

### Post-Migration Verification

After migration, verify data integrity:

```bash
# Compare row counts
echo "QuestDB:"
curl -s -G 'http://questdb-host:9000/exec' \
  --data-urlencode 'query=SELECT count() FROM trades' | jq '.dataset[0][0]'

echo "ExchangeDB:"
curl -s -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT count() FROM trades"}' | jq '.rows[0][0]'

# Compare min/max timestamps
curl -s -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT min(timestamp), max(timestamp) FROM trades"}'

# Verify a sample query works
curl -s -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT symbol, count(), avg(price) FROM trades GROUP BY symbol"}'
```

---

## Client Library Changes

### Python

**Before (QuestDB):**

```python
import requests

# Query
resp = requests.get("http://localhost:9000/exec", params={"query": sql})
data = resp.json()["dataset"]

# ILP (TCP) -- no change needed
from questdb.ingress import Sender
with Sender("localhost", 9009) as sender:
    sender.row("trades", symbols={"symbol": "BTC/USD"}, columns={"price": 65000.0})
    sender.flush()
```

**After (ExchangeDB):**

```python
import requests

# Query -- different URL and method
resp = requests.post("http://localhost:9000/api/v1/query", json={"query": sql})
data = resp.json()["rows"]

# ILP (TCP) -- no change needed, same protocol
# You can continue using questdb.ingress or any ILP sender
```

### Node.js

**Before (QuestDB):**

```javascript
const resp = await fetch(`http://localhost:9000/exec?query=${encodeURIComponent(sql)}`);
const data = await resp.json();
const rows = data.dataset;
```

**After (ExchangeDB):**

```javascript
const resp = await fetch('http://localhost:9000/api/v1/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ query: sql }),
});
const data = await resp.json();
const rows = data.rows;
```

### Go

**Before (QuestDB):**

```go
resp, _ := http.Get(fmt.Sprintf("http://localhost:9000/exec?query=%s", url.QueryEscape(sql)))
```

**After (ExchangeDB):**

```go
body, _ := json.Marshal(map[string]string{"query": sql})
resp, _ := http.Post("http://localhost:9000/api/v1/query", "application/json", bytes.NewReader(body))
```

---

## Feature Comparison

| Feature | QuestDB | ExchangeDB | Notes |
|---------|---------|------------|-------|
| `SAMPLE BY` | Yes | Yes | Identical syntax |
| `LATEST ON` | Yes | Yes | Identical syntax |
| `ASOF JOIN` | Yes | Yes | Identical syntax |
| `SPLICE JOIN` | Yes | Not yet | Check roadmap |
| `FILL` | Yes | Yes | Identical syntax |
| `WHERE IN` | Yes | Yes | Identical |
| `LIMIT` / `OFFSET` | Yes | Yes | Identical |
| `GROUP BY` | Yes | Yes | Identical |
| `ORDER BY` | Yes | Yes | Identical |
| `DISTINCT` | Yes | Yes | Identical |
| `UNION ALL` | Yes | Yes | Identical |
| `UPDATE` | Limited | Limited | Check current support |
| `DELETE` | Limited | Limited | Check current support |
| `CREATE INDEX` | Via SYMBOL | Via SYMBOL | Automatic for SYMBOL columns |
| Deduplication | WAL dedup | WAL dedup | Similar mechanisms |
| Out-of-order ingestion | Yes | Yes | O3 support |
| Detached partitions | Yes | Snapshot/Restore | Different mechanism |
| JIT compilation | Yes (SQL JIT) | No | ExchangeDB uses compiled filters |
| Web console | Yes | Yes | Built-in at `/` |
| Grafana plugin | Yes | Via pgwire | Use PostgreSQL Grafana datasource |
| Prometheus metrics | `/metrics` | `/metrics` | Compatible |
| Kubernetes operator | Community | Not yet | Use standard Docker/Compose |
| Enterprise features | Enterprise license | Built-in | Auth, RBAC, replication included |
| TLS/HTTPS | Enterprise | Built-in | Configuration-based |
| Replication | Enterprise | Built-in | Primary/replica with async/sync modes |
| RBAC | Enterprise | Built-in | Role-based access control |
