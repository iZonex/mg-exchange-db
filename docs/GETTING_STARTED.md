# Getting Started with ExchangeDB

ExchangeDB is a high-performance columnar time-series database written in Rust,
optimized for financial exchange workloads: tick data, OHLCV bars, order book
snapshots, trades, and market events.

This guide walks you through installation, creating your first table, ingesting
data, running queries, and connecting from popular clients.

---

## Table of Contents

1. [Installation](#installation)
2. [Starting the Server](#starting-the-server)
3. [Creating Your First Table](#creating-your-first-table)
4. [Inserting Data](#inserting-data)
5. [Your First Query](#your-first-query)
6. [Connecting Clients](#connecting-clients)
7. [Web Console](#web-console)
8. [Next Steps](#next-steps)

---

## Installation

### Option 1: Build from Source

**Prerequisites:**

- Rust 1.85 or later (install via [rustup](https://rustup.rs/))
- A C linker (gcc or clang)
- OpenSSL development headers
  - Debian/Ubuntu: `sudo apt install libssl-dev pkg-config`
  - macOS: included with Xcode Command Line Tools
  - Fedora/RHEL: `sudo dnf install openssl-devel`

**Build:**

```bash
git clone https://github.com/your-org/exchange-db.git
cd exchange-db
cargo build --release
```

The compiled binary is located at `target/release/exchange-db`. Optionally
install it to your PATH:

```bash
sudo cp target/release/exchange-db /usr/local/bin/
```

Verify the installation:

```bash
exchange-db --version
```

### Option 2: Docker

**Using Docker Compose (recommended):**

```bash
git clone https://github.com/your-org/exchange-db.git
cd exchange-db
docker compose up -d
```

This starts ExchangeDB with persistent storage via a named volume, health checks,
and all three ports exposed (HTTP 9000, pgwire 8812, ILP 9009).

**Using Docker directly:**

```bash
# Build the image
docker build -t exchangedb .

# Run with persistent storage
docker run -d --name exchangedb \
  -p 9000:9000 \
  -p 8812:8812 \
  -p 9009:9009 \
  -v exchangedb-data:/data \
  exchangedb
```

**Docker environment variables:**

| Variable | Description | Default |
|----------|-------------|---------|
| `EXCHANGEDB_DATA_DIR` | Data directory inside container | `/data` |
| `EXCHANGEDB_LOG_LEVEL` | Log level | `info` |
| `EXCHANGEDB_HTTP_BIND` | HTTP bind address | `0.0.0.0:9000` |
| `EXCHANGEDB_PGWIRE_BIND` | PostgreSQL wire protocol bind | `0.0.0.0:8812` |
| `EXCHANGEDB_ILP_BIND` | ILP bind address | `0.0.0.0:9009` |

### Option 3: Pre-built Binary

Download the latest release binary for your platform from the
[releases page](https://github.com/your-org/exchange-db/releases). Extract and
run:

```bash
tar xzf exchangedb-linux-amd64.tar.gz
sudo mv exchange-db /usr/local/bin/
exchange-db server
```

---

## Starting the Server

### Default Configuration

Start ExchangeDB with all protocols enabled using defaults:

```bash
exchange-db server
```

This binds:

| Protocol | Address | Purpose |
|----------|---------|---------|
| HTTP REST + Web Console | `0.0.0.0:9000` | Queries, admin, web UI |
| PostgreSQL Wire Protocol | `0.0.0.0:8812` | psql, DBeaver, Grafana |
| ILP TCP/UDP | `0.0.0.0:9009` | High-throughput ingestion |

Data is stored in `./data` by default.

### Custom Configuration

```bash
# Specify a config file
exchange-db server --config /etc/exchangedb/exchange-db.toml

# Override bind address and data directory
exchange-db server --bind 127.0.0.1:9000 --data-dir /var/lib/exchangedb
```

### Verify the Server Is Running

```bash
curl http://localhost:9000/api/v1/health
```

Expected response:

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_secs": 2.45
}
```

---

## Creating Your First Table

### Via psql

```bash
psql -h localhost -p 8812

CREATE TABLE trades (
    timestamp TIMESTAMP,
    symbol    SYMBOL,
    price     DOUBLE,
    volume    DOUBLE,
    side      SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

Key concepts:

- **TIMESTAMP(column)** designates the time column used for partitioning and
  time-series operations.
- **PARTITION BY DAY** partitions data into daily directories on disk. Options:
  `NONE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `YEAR`.
- **SYMBOL** is a dictionary-encoded string type, ideal for low-cardinality
  columns like ticker symbols. It is automatically indexed.

### Via HTTP

```bash
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "CREATE TABLE trades (timestamp TIMESTAMP, symbol SYMBOL, price DOUBLE, volume DOUBLE, side SYMBOL) TIMESTAMP(timestamp) PARTITION BY DAY"
  }'
```

### Via CLI

```bash
exchange-db sql "CREATE TABLE trades (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    price DOUBLE,
    volume DOUBLE,
    side SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY"
```

---

## Inserting Data

### SQL INSERT

**Single row:**

```sql
INSERT INTO trades VALUES (
    '2024-03-01T10:00:00.000000Z',
    'BTC/USD',
    65000.0,
    1.5,
    'buy'
);
```

**Multiple rows:**

```sql
INSERT INTO trades VALUES
    ('2024-03-01T10:00:00Z', 'BTC/USD', 65000.0, 1.5, 'buy'),
    ('2024-03-01T10:00:01Z', 'ETH/USD', 3400.0, 10.0, 'sell'),
    ('2024-03-01T10:00:02Z', 'BTC/USD', 65010.5, 0.8, 'buy');
```

**INSERT from SELECT:**

```sql
INSERT INTO trades_archive
SELECT * FROM trades WHERE timestamp < '2024-01-01';
```

### InfluxDB Line Protocol (ILP) over TCP

ILP is the fastest ingestion method. Send newline-terminated lines to port 9009:

```bash
echo "trades,symbol=BTC/USD,side=buy price=65000.0,volume=1.5 1709283600000000000
trades,symbol=ETH/USD,side=sell price=3400.0,volume=10.0 1709283601000000000" \
  | nc localhost 9009
```

**ILP format:**

```
measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 [timestamp_ns]
```

- Tags become SYMBOL columns (auto-indexed).
- Fields become typed columns (float by default, integer with `i` suffix, string
  in quotes, boolean as `true`/`false`).
- Timestamp is optional (defaults to `now()`) and must be in nanoseconds.
- Tables are auto-created on first write.

### ILP over HTTP

```bash
curl -X POST http://localhost:9000/api/v1/write \
  -d 'trades,symbol=BTC/USD,side=buy price=65000.0,volume=1.5 1709283600000000000'
```

### CSV Import

**Via CLI:**

```bash
exchange-db import --table trades --file trades.csv
```

**Via HTTP:**

```bash
curl -X POST 'http://localhost:9000/api/v1/import?table=trades' \
  -H 'Content-Type: text/csv' \
  --data-binary @trades.csv
```

**Via SQL:**

```sql
COPY trades FROM '/path/to/trades.csv' WITH (FORMAT CSV, HEADER TRUE);
```

---

## Your First Query

### Basic SELECT

```sql
SELECT * FROM trades LIMIT 10;
```

### Filtering by Time

```sql
SELECT symbol, price, volume
FROM trades
WHERE timestamp BETWEEN '2024-03-01' AND '2024-03-02'
  AND symbol = 'BTC/USD'
ORDER BY timestamp DESC
LIMIT 100;
```

### Time Bucketing with SAMPLE BY

Aggregate data into fixed time intervals:

```sql
SELECT symbol,
    first(price)  AS open,
    max(price)    AS high,
    min(price)    AS low,
    last(price)   AS close,
    sum(volume)   AS volume
FROM trades
WHERE symbol = 'BTC/USD'
SAMPLE BY 1h;
```

Supported intervals: `s` (seconds), `m` (minutes), `h` (hours), `d` (days),
`w` (weeks), `M` (months), `y` (years).

### Latest Value per Symbol

```sql
SELECT * FROM trades
LATEST ON timestamp PARTITION BY symbol;
```

### Temporal Join (ASOF JOIN)

Join trades with the most recent quote at each trade's timestamp:

```sql
SELECT t.timestamp, t.symbol, t.price, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q ON (t.symbol = q.symbol);
```

### Financial Aggregates

```sql
SELECT symbol,
    vwap(price, volume) AS vwap,
    ema(price, 20) AS ema_20,
    rsi(price, 14) AS rsi_14
FROM trades
GROUP BY symbol;
```

---

## Connecting Clients

### psql (PostgreSQL CLI)

```bash
psql -h localhost -p 8812 -d exchangedb
```

No username or password is required by default (authentication is disabled).

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
    print(f"{row[0]}: avg_price={row[1]:.2f}, total_vol={row[2]:.2f}")

conn.close()
```

### Python (HTTP via requests)

```python
import requests

resp = requests.post("http://localhost:9000/api/v1/query", json={
    "query": "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol"
})

data = resp.json()
for col in data["columns"]:
    print(f"{col['name']}: {col['type']}")
for row in data["rows"]:
    print(row)
```

### curl

```bash
# Query
curl -s -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM trades LIMIT 5"}' | jq

# Export CSV
curl -o trades.csv \
  'http://localhost:9000/api/v1/export?query=SELECT+*+FROM+trades'

# Health check
curl http://localhost:9000/api/v1/health
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
const res = await client.query('SELECT * FROM trades SAMPLE BY 1h');
console.log(res.rows);
await client.end();
```

### Java (JDBC)

```java
Connection conn = DriverManager.getConnection(
    "jdbc:postgresql://localhost:8812/exchangedb"
);
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery(
    "SELECT symbol, avg(price) FROM trades GROUP BY symbol"
);
while (rs.next()) {
    System.out.printf("%s: %.2f%n",
        rs.getString("symbol"), rs.getDouble(2));
}
conn.close();
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
        "SELECT symbol, avg(price) FROM trades GROUP BY symbol"
    )
    .fetch_all(&pool)
    .await?;

    for (symbol, avg_price) in rows {
        println!("{symbol}: {avg_price:.2}");
    }
    Ok(())
}
```

### Grafana

1. Add a new data source in Grafana.
2. Select **PostgreSQL**.
3. Set host to `localhost:8812`, database to `exchangedb`.
4. Leave username and password empty (or configure if auth is enabled).
5. Use the query editor with ExchangeDB SQL, including `SAMPLE BY`.

### DBeaver

1. Create a new PostgreSQL connection.
2. Set host to `localhost`, port to `8812`, database to `exchangedb`.
3. Leave authentication fields empty.
4. Test and connect.

---

## Web Console

ExchangeDB includes a built-in web console. Open your browser and navigate to:

```
http://localhost:9000
```

### Features

- **SQL editor** with dark theme and syntax highlighting.
- **Table browser** in the sidebar, auto-populated from the database.
- **Result viewer** rendering query results in a scrollable table.
- **Keyboard shortcut**: Press `Ctrl+Enter` (or `Cmd+Enter` on macOS) to
  execute the query.
- **Timing display** showing execution duration in milliseconds.
- **Error display** with red highlighting for SQL errors.

### Using the Console

1. Open `http://localhost:9000` in any modern browser.
2. Type a SQL query in the editor pane, for example:

   ```sql
   SELECT symbol,
       first(price) AS open,
       max(price)   AS high,
       min(price)   AS low,
       last(price)  AS close,
       sum(volume)  AS volume
   FROM trades
   SAMPLE BY 1h;
   ```

3. Press `Ctrl+Enter` to run.
4. View results in the table below the editor.
5. Click table names in the sidebar to see their schema.

---

## Next Steps

- **[SQL Reference](SQL_REFERENCE.md)** -- Complete SQL syntax reference.
- **[Functions Reference](FUNCTIONS_REFERENCE.md)** -- All 1,198+ functions.
- **[Data Types](DATA_TYPES.md)** -- All 41 supported data types.
- **[Ingestion Guide](INGESTION.md)** -- ILP, CSV, and bulk loading best practices.
- **[Operations Guide](OPERATIONS.md)** -- Backup, monitoring, retention, WAL management.
- **[Configuration](CONFIGURATION_COMPLETE.md)** -- Every configuration option.
- **[Security Guide](SECURITY.md)** -- Authentication, RBAC, TLS, encryption.
- **[Replication Guide](REPLICATION.md)** -- Primary-replica setup and failover.
- **[Troubleshooting](TROUBLESHOOTING.md)** -- Common issues and solutions.
