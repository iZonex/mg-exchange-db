# ExchangeDB API Reference

ExchangeDB exposes three network interfaces:

1. **HTTP REST API** -- Port 9000 (default)
2. **PostgreSQL Wire Protocol (pgwire)** -- Port 8812 (default)
3. **InfluxDB Line Protocol (ILP)** -- Port 9009 (default)

This document covers every HTTP endpoint in detail.

---

## Table of Contents

- [Base URL](#base-url)
- [Authentication](#authentication)
- [Error Format](#error-format)
- [Core API (`/api/v1`)](#core-api)
  - [GET /api/v1/health](#get-apiv1health)
  - [POST /api/v1/query](#post-apiv1query)
  - [GET /api/v1/query/stream](#get-apiv1querystream)
  - [POST /api/v1/write](#post-apiv1write)
  - [GET /api/v1/tables](#get-apiv1tables)
  - [GET /api/v1/tables/{name}](#get-apiv1tablesname)
  - [GET /api/v1/export](#get-apiv1export)
  - [POST /api/v1/import](#post-apiv1import)
  - [GET /api/v1/diagnostics](#get-apiv1diagnostics)
  - [GET /api/v1/ws](#get-apiv1ws)
- [Admin API (`/admin`)](#admin-api)
  - [GET /admin/config](#get-adminconfig)
  - [POST /admin/config](#post-adminconfig)
  - [GET /admin/users](#get-adminusers)
  - [POST /admin/users](#post-adminusers)
  - [DELETE /admin/users/{name}](#delete-adminusersname)
  - [GET /admin/roles](#get-adminroles)
  - [POST /admin/roles](#post-adminroles)
  - [GET /admin/cluster](#get-admincluster)
  - [GET /admin/replication](#get-adminreplication)
  - [GET /admin/wal](#get-adminwal)
  - [GET /admin/partitions/{table}](#get-adminpartitionstable)
  - [POST /admin/vacuum/{table}](#post-adminvacuumtable)
  - [POST /admin/checkpoint](#post-admincheckpoint)
  - [GET /admin/slow-queries](#get-adminslow-queries)
  - [GET /admin/jobs](#get-adminjobs)
- [Auth API (`/auth`)](#auth-api)
  - [GET /auth/login](#get-authlogin)
  - [GET /auth/callback](#get-authcallback)
  - [GET /auth/token](#get-authtoken)
  - [POST /auth/logout](#post-authlogout)
- [Metrics](#metrics)
  - [GET /metrics](#get-metrics)
- [Web Console](#web-console)
  - [GET /](#get-)

---

## Base URL

```
http://localhost:9000
```

All API endpoints are prefixed accordingly. CORS is enabled for all origins by default.

## Authentication

When `security.auth_enabled` is `false` (default), all endpoints are accessible without credentials.

When authentication is enabled, include a bearer token in the `Authorization` header:

```
Authorization: Bearer <token>
```

Auth-related endpoints (`/auth/*`) are always public.

## Error Format

All errors return a JSON body:

```json
{
  "error": "description of the error",
  "code": 400,
  "sql_state": "42601",
  "query": "SELECT * FROM nonexistent"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `error` | string | Human-readable error message |
| `code` | integer | HTTP status code |
| `sql_state` | string (optional) | PostgreSQL-compatible SQLSTATE code |
| `query` | string (optional) | The SQL query that caused the error |

### SQLSTATE Codes

| Code | Meaning |
|------|---------|
| `42P01` | Table not found |
| `42703` | Column not found |
| `42P07` | Table already exists |
| `42601` | Syntax error |
| `42804` | Data type mismatch |
| `42501` | Permission denied |
| `42000` | General bad request |
| `53300` | Resource exhausted / too many requests |
| `XX000` | Internal error |

### HTTP Status Codes

| Status | Meaning |
|--------|---------|
| 200 | Success |
| 201 | Created (user/role creation) |
| 400 | Bad request (parse error, empty query, etc.) |
| 403 | Forbidden (replica read-only, permission denied) |
| 404 | Not found (table does not exist) |
| 409 | Conflict (table/user already exists) |
| 429 | Too many requests (resource exhausted) |
| 500 | Internal server error |

---

## Core API

### GET /api/v1/health

Returns the health status of the server.

**Request:**

```bash
curl http://localhost:9000/api/v1/health
```

**Response (200):**

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_secs": 3600.5
}
```

When replication is configured, includes replication status:

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_secs": 3600.5,
  "replication": {
    "role": "primary",
    "lag_bytes": 0,
    "connected_replicas": 2
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Always `"ok"` when server is healthy |
| `version` | string | Server version |
| `uptime_secs` | float | Seconds since server start |
| `replication` | object (optional) | Present when replication is configured |

---

### POST /api/v1/query

Executes a SQL query and returns results as JSON.

**Request:**

```bash
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT symbol, price, volume FROM trades LIMIT 5"}'
```

**Request Body:**

```json
{
  "query": "SELECT symbol, price, volume FROM trades LIMIT 5"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | Yes | SQL query to execute |

**Response (200) -- SELECT:**

```json
{
  "columns": [
    {"name": "symbol", "type": "Varchar"},
    {"name": "price", "type": "Varchar"},
    {"name": "volume", "type": "Varchar"}
  ],
  "rows": [
    ["BTC/USD", 65000.0, 1.5],
    ["ETH/USD", 3200.0, 10.0]
  ],
  "timing_ms": 1.23
}
```

**Response (200) -- INSERT/CREATE/DDL:**

```json
{
  "columns": [
    {"name": "affected_rows", "type": "I64"}
  ],
  "rows": [[1]],
  "timing_ms": 0.45
}
```

| Field | Type | Description |
|-------|------|-------------|
| `columns` | array | Column metadata (name and type) |
| `rows` | array of arrays | Row data, each row is an array of values |
| `timing_ms` | float | Query execution time in milliseconds |

**Error (400):**

```json
{
  "error": "query must not be empty",
  "code": 400,
  "sql_state": "42000"
}
```

**Examples:**

```bash
# Create table
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE) TIMESTAMP(timestamp) PARTITION BY DAY"}'

# Insert data
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "INSERT INTO trades VALUES (now(), '\''BTC/USD'\'', 65000.0, 1.5)"}'

# Aggregation query
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT symbol, avg(price), sum(volume) FROM trades GROUP BY symbol"}'

# Drop table
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "DROP TABLE IF EXISTS trades"}'
```

---

### GET /api/v1/query/stream

Executes a SQL query and streams results as newline-delimited JSON (NDJSON). Useful for large result sets.

**Request:**

```bash
curl 'http://localhost:9000/api/v1/query/stream?query=SELECT+*+FROM+trades+LIMIT+1000'
```

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | URL-encoded SQL query |

**Response (200):**

Content-Type: `application/x-ndjson`

Each line is a separate JSON object:

```
{"columns":["timestamp","symbol","price","volume"]}
{"row":[1710513000000000000,"BTC/USD",65000.0,1.5]}
{"row":[1710513001000000000,"ETH/USD",3200.0,10.0]}
{"complete":true,"row_count":2}
```

| Line Type | Description |
|-----------|-------------|
| First line | `{"columns": [...]}` -- column names |
| Data lines | `{"row": [...]}` -- one per row |
| Last line | `{"complete": true, "row_count": N}` -- completion marker |

**Example:**

```bash
curl -s 'http://localhost:9000/api/v1/query/stream?query=SELECT+*+FROM+trades' | while read line; do
  echo "$line" | jq .
done
```

---

### POST /api/v1/write

Ingests data via InfluxDB Line Protocol (ILP) over HTTP.

**Request:**

```bash
curl -X POST http://localhost:9000/api/v1/write \
  -d 'trades,symbol=BTC/USD price=65000.0,volume=1.5
trades,symbol=ETH/USD price=3200.0,volume=10.0 1710513000000000000'
```

**Request Body:**

Plain text, one ILP line per line. Format:

```
measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 [timestamp_ns]
```

- Tags are stored as indexed SYMBOL columns.
- Fields are stored with inferred types (integer `i` suffix, float, string in quotes, boolean).
- Timestamp is optional (defaults to `now()`) and is in nanoseconds.

**Response (200):**

```json
{
  "status": "ok",
  "lines_accepted": 2
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `"ok"` on success |
| `lines_accepted` | integer | Number of ILP lines ingested |

**Error (400):**

```json
{
  "error": "ILP parse error: invalid line at position 3",
  "code": 400
}
```

**Error (403) -- on read-only replica:**

```json
{
  "error": "this is a read-only replica",
  "code": 403
}
```

**ILP Format Details:**

```
# Integer field (note the 'i' suffix)
trades,symbol=BTC/USD price=65000i,volume=15i

# Float field (no suffix, or with decimal point)
trades,symbol=BTC/USD price=65000.0,volume=1.5

# String field (quoted)
events,type=order message="filled at 65000"

# Boolean field
signals,symbol=BTC/USD active=true

# With explicit nanosecond timestamp
trades,symbol=BTC/USD price=65000.0 1710513000000000000
```

Tables are auto-created if they don't exist. The schema is inferred from the first line's tags and fields.

---

### GET /api/v1/tables

Lists all tables in the database.

**Request:**

```bash
curl http://localhost:9000/api/v1/tables
```

**Response (200):**

```json
{
  "tables": ["orderbook", "quotes", "trades"]
}
```

---

### GET /api/v1/tables/{name}

Returns metadata for a specific table.

**Request:**

```bash
curl http://localhost:9000/api/v1/tables/trades
```

**Response (200):**

```json
{
  "name": "trades",
  "columns": [
    {"name": "timestamp", "type": "Timestamp"},
    {"name": "symbol", "type": "Symbol"},
    {"name": "price", "type": "F64"},
    {"name": "volume", "type": "F64"}
  ],
  "partition_by": "Day",
  "row_count": 1500000
}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Table name |
| `columns` | array | Column definitions |
| `partition_by` | string | Partition strategy |
| `row_count` | integer | Approximate total row count |

**Error (404):**

```json
{
  "error": "table 'nonexistent' not found",
  "code": 404,
  "sql_state": "42P01"
}
```

---

### GET /api/v1/export

Exports query results as CSV.

**Request:**

```bash
curl 'http://localhost:9000/api/v1/export?query=SELECT+*+FROM+trades+LIMIT+100&format=csv'
```

**Query Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `query` | string | Yes | -- | URL-encoded SQL query |
| `format` | string | No | `csv` | Export format (only `csv` is supported) |

**Response (200):**

Content-Type: `text/csv`
Content-Disposition: `attachment; filename="export.csv"`

```csv
timestamp,symbol,price,volume
1710513000000000000,BTC/USD,65000.0,1.5
1710513001000000000,ETH/USD,3200.0,10.0
```

**Error (400):**

```json
{
  "error": "unsupported export format: 'json'. Only 'csv' is supported.",
  "code": 400
}
```

---

### POST /api/v1/import

Imports CSV data into a table (creates the table if it doesn't exist).

**Request:**

```bash
curl -X POST 'http://localhost:9000/api/v1/import?table=trades' \
  -H 'Content-Type: text/csv' \
  -d 'timestamp,symbol,price,volume
1710513000000000000,BTC/USD,65000.0,1.5
1710513001000000000,ETH/USD,3200.0,10.0'
```

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `table` | string | Yes | Target table name |

**Request Body:**

CSV text with a header row followed by data rows.

**Response (200):**

```json
{
  "rows_imported": 2,
  "table": "trades"
}
```

**Error (400):**

```json
{
  "error": "CSV header is empty",
  "code": 400
}
```

---

### GET /api/v1/diagnostics

Returns comprehensive system diagnostics.

**Request:**

```bash
curl http://localhost:9000/api/v1/diagnostics
```

**Response (200):**

```json
{
  "version": "0.1.0",
  "rust_version": "1.85.0",
  "os": "linux",
  "arch": "x86_64",
  "pid": 12345,
  "uptime_secs": 3600.5,
  "memory": {
    "rss_bytes": 104857600,
    "heap_bytes": 0
  },
  "storage": {
    "data_dir": "/data",
    "disk_free_bytes": 536870912000,
    "tables": 3,
    "total_rows": 15000000
  },
  "connections": {
    "http": 5,
    "pgwire": 2,
    "ilp": 1
  },
  "wal": {
    "pending_segments": 0,
    "applied_segments": 42
  },
  "config": {
    "http_port": 9000,
    "pg_port": 8812,
    "ilp_port": 9009
  }
}
```

---

### GET /api/v1/ws

WebSocket endpoint for real-time data subscriptions. Connect with a WebSocket client and subscribe to table changes.

**Connection:**

```javascript
const ws = new WebSocket('ws://localhost:9000/api/v1/ws');

ws.onopen = () => {
  // Subscribe to a table
  ws.send(JSON.stringify({ subscribe: 'trades' }));
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('New data:', data);
};
```

When data is written to a subscribed table (via ILP or HTTP write), the WebSocket receives JSON notifications with the new rows.

---

## Admin API

All admin endpoints are mounted under `/admin/`. When RBAC is enabled, these endpoints require the `Admin` permission.

### GET /admin/config

Returns the current server configuration.

```bash
curl http://localhost:9000/admin/config
```

**Response (200):**

```json
{
  "data_dir": "/data",
  "read_only": false,
  "uptime_secs": 3600.5
}
```

---

### POST /admin/config

Updates runtime configuration (hot reload).

```bash
curl -X POST http://localhost:9000/admin/config \
  -H 'Content-Type: application/json' \
  -d '{"log_level": "debug"}'
```

**Request Body:**

```json
{
  "log_level": "debug"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `log_level` | string | No | New log level (trace, debug, info, warn, error) |

**Response (200):**

```json
{
  "status": "ok",
  "message": "log_level update requested: debug"
}
```

---

### GET /admin/users

Lists all users.

```bash
curl http://localhost:9000/admin/users
```

**Response (200):**

```json
[
  {
    "username": "admin",
    "roles": ["admin"],
    "enabled": true
  },
  {
    "username": "reader",
    "roles": ["read_only"],
    "enabled": true
  }
]
```

---

### POST /admin/users

Creates a new user.

```bash
curl -X POST http://localhost:9000/admin/users \
  -H 'Content-Type: application/json' \
  -d '{
    "username": "alice",
    "password": "secure_password_123",
    "roles": ["reader", "writer"]
  }'
```

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `username` | string | Yes | Username |
| `password` | string | Yes | Plain-text password (hashed before storage) |
| `roles` | array of strings | No | Role names to assign |

**Response (201):**

```json
{
  "status": "ok",
  "message": "user 'alice' created"
}
```

**Error (409):**

```json
{
  "error": "user 'alice' already exists",
  "code": 409
}
```

---

### DELETE /admin/users/{name}

Deletes a user.

```bash
curl -X DELETE http://localhost:9000/admin/users/alice
```

**Response (200):**

```json
{
  "status": "ok",
  "message": "user 'alice' deleted"
}
```

---

### GET /admin/roles

Lists all roles.

```bash
curl http://localhost:9000/admin/roles
```

**Response (200):**

```json
[
  {
    "name": "reader",
    "permissions": ["Read { table: None }"]
  },
  {
    "name": "writer",
    "permissions": ["Read { table: None }", "Write { table: None }"]
  }
]
```

---

### POST /admin/roles

Creates a new role.

```bash
curl -X POST http://localhost:9000/admin/roles \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "trader",
    "permissions": ["read", "write:trades", "write:orders"]
  }'
```

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Role name |
| `permissions` | array of strings | No | Permission strings |

**Permission Strings:**

| Permission | Description |
|------------|-------------|
| `admin` | Full administrative access |
| `ddl` | CREATE, DROP, ALTER tables |
| `system` | System operations |
| `read` | Read all tables |
| `read:<table>` | Read specific table |
| `write` | Write to all tables |
| `write:<table>` | Write to specific table |

**Response (201):**

```json
{
  "status": "ok",
  "message": "role 'trader' created"
}
```

---

### GET /admin/cluster

Returns cluster status.

```bash
curl http://localhost:9000/admin/cluster
```

**Response (200):**

```json
{
  "node_id": "local",
  "role": "standalone",
  "status": "healthy"
}
```

---

### GET /admin/replication

Returns replication status.

```bash
curl http://localhost:9000/admin/replication
```

**Response (200):**

```json
{
  "role": "primary",
  "lag_bytes": 0,
  "lag_seconds": 0,
  "segments_shipped": 42,
  "segments_applied": 42
}
```

---

### GET /admin/wal

Returns WAL status.

```bash
curl http://localhost:9000/admin/wal
```

**Response (200):**

```json
{
  "wal_enabled": true,
  "total_segments": 15,
  "total_bytes": 67108864
}
```

---

### GET /admin/partitions/{table}

Returns partition information for a table.

```bash
curl http://localhost:9000/admin/partitions/trades
```

**Response (200):**

```json
{
  "table": "trades",
  "partitions": [
    {
      "name": "2024-03-14",
      "row_count": 50000,
      "size_bytes": 4194304
    },
    {
      "name": "2024-03-15",
      "row_count": 75000,
      "size_bytes": 6291456
    }
  ]
}
```

**Error (404):**

```json
{
  "error": "table 'nonexistent' not found",
  "code": 404
}
```

---

### POST /admin/vacuum/{table}

Triggers a VACUUM operation on a table (compacts storage).

```bash
curl -X POST http://localhost:9000/admin/vacuum/trades
```

**Response (200):**

```json
{
  "table": "trades",
  "status": "completed"
}
```

---

### POST /admin/checkpoint

Triggers a WAL checkpoint (flushes pending WAL to tables).

```bash
curl -X POST http://localhost:9000/admin/checkpoint
```

**Response (200):**

```json
{
  "status": "completed"
}
```

---

### GET /admin/slow-queries

Returns recent slow queries.

```bash
curl http://localhost:9000/admin/slow-queries
```

**Response (200):**

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

---

### GET /admin/jobs

Returns background job status.

```bash
curl http://localhost:9000/admin/jobs
```

**Response (200):**

```json
{
  "jobs": [
    {
      "name": "retention_check",
      "status": "idle",
      "last_run": null
    },
    {
      "name": "wal_checkpoint",
      "status": "idle",
      "last_run": null
    }
  ]
}
```

---

## Auth API

Authentication endpoints are always accessible without credentials.

### GET /auth/login

Initiates OAuth login flow. Redirects the user to the OAuth provider.

```bash
curl -v http://localhost:9000/auth/login
```

### GET /auth/callback

OAuth callback endpoint. The OAuth provider redirects here with an authorization code.

### GET /auth/token

Returns information about the current authentication token.

```bash
curl http://localhost:9000/auth/token \
  -H 'Authorization: Bearer <token>'
```

### POST /auth/logout

Invalidates the current session/token.

```bash
curl -X POST http://localhost:9000/auth/logout \
  -H 'Authorization: Bearer <token>'
```

---

## Metrics

### GET /metrics

Prometheus-compatible metrics endpoint.

```bash
curl http://localhost:9000/metrics
```

**Response (200):**

Content-Type: `text/plain`

```
# HELP exchangedb_queries_total Total number of queries executed.
# TYPE exchangedb_queries_total counter
exchangedb_queries_total 12345

# HELP exchangedb_queries_failed_total Total number of failed queries.
# TYPE exchangedb_queries_failed_total counter
exchangedb_queries_failed_total 3

# HELP exchangedb_query_duration_seconds Query execution time.
# TYPE exchangedb_query_duration_seconds histogram
exchangedb_query_duration_seconds_bucket{le="0.001"} 5000
exchangedb_query_duration_seconds_bucket{le="0.01"} 10000
exchangedb_query_duration_seconds_bucket{le="0.1"} 11500
exchangedb_query_duration_seconds_bucket{le="1.0"} 12000
exchangedb_query_duration_seconds_bucket{le="10.0"} 12300
exchangedb_query_duration_seconds_bucket{le="+Inf"} 12345
exchangedb_query_duration_seconds_sum 450.25
exchangedb_query_duration_seconds_count 12345

# HELP exchangedb_rows_written_total Total rows written.
# TYPE exchangedb_rows_written_total counter
exchangedb_rows_written_total 5000000

# HELP exchangedb_rows_read_total Total rows read.
# TYPE exchangedb_rows_read_total counter
exchangedb_rows_read_total 50000000

# HELP exchangedb_ilp_lines_total Total ILP lines ingested.
# TYPE exchangedb_ilp_lines_total counter
exchangedb_ilp_lines_total 4500000

# HELP exchangedb_active_connections Current active HTTP connections.
# TYPE exchangedb_active_connections gauge
exchangedb_active_connections 5

# HELP exchangedb_tables_count Number of tables.
# TYPE exchangedb_tables_count gauge
exchangedb_tables_count 3
```

---

## Web Console

### GET /

Serves a built-in web console for interactive SQL queries. Open `http://localhost:9000/` in a browser.

---

## PostgreSQL Wire Protocol

ExchangeDB implements the PostgreSQL wire protocol on port 8812. Connect with any PostgreSQL client:

```bash
psql -h localhost -p 8812 -d exchangedb
```

All SQL statements supported by the HTTP API are also available via pgwire, including:
- `SELECT`, `INSERT`, `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`
- `SAMPLE BY`, `LATEST ON`, `ASOF JOIN`
- Extended query protocol (prepared statements, parameterized queries)
- `COPY` protocol for bulk data loading

---

## InfluxDB Line Protocol (TCP)

ExchangeDB accepts ILP over TCP on port 9009. Send newline-terminated ILP lines:

```bash
echo 'trades,symbol=BTC/USD price=65000.0,volume=1.5 1710513000000000000
trades,symbol=ETH/USD price=3200.0,volume=10.0' | nc localhost 9009
```

ILP format:

```
<measurement>,<tag1>=<val1>,<tag2>=<val2> <field1>=<val1>,<field2>=<val2> [timestamp_ns]
```

- Tags: key=value pairs after measurement name (stored as SYMBOL, auto-indexed)
- Fields: key=value pairs after the space (types inferred)
- Timestamp: optional, nanoseconds since Unix epoch
- Tables auto-created on first write with schema inferred from the first line
