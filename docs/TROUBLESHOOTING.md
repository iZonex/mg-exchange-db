# ExchangeDB Troubleshooting Guide

This guide covers common issues, error messages, and solutions for ExchangeDB.
It addresses problems inspired by real-world time-series database issues
including those reported in QuestDB's issue tracker.

---

## Table of Contents

1. [Table Locked After OOM](#table-locked-after-oom)
2. [SQL Injection Prevention](#sql-injection-prevention)
3. [Timezone and DST Issues](#timezone-and-dst-issues)
4. [LIKE Escape Characters](#like-escape-characters)
5. [Case Sensitivity](#case-sensitivity)
6. [Connection Issues](#connection-issues)
7. [Performance Tuning](#performance-tuning)
8. [WAL Issues](#wal-issues)
9. [Disk Space Management](#disk-space-management)
10. [Common SQL Errors](#common-sql-errors)
11. [Data Type Issues](#data-type-issues)
12. [Ingestion Issues](#ingestion-issues)
13. [Memory Issues](#memory-issues)
14. [Startup Problems](#startup-problems)

---

## Table Locked After OOM

**Problem:** After an out-of-memory (OOM) event, a table becomes locked and
queries or writes fail with a "table locked" or "write lock held" error.

**Related:** QuestDB issue #1645

### ExchangeDB's Approach

ExchangeDB uses per-table write locks with timeout-based recovery:

1. Each table has a `WriteLock` that is held during write operations.
2. If the server crashes or is OOM-killed during a write, the lock file
   may remain on disk.
3. On restart, ExchangeDB's crash recovery process:
   - Detects stale lock files.
   - Replays incomplete WAL segments.
   - Releases stale locks.
   - Runs a consistency check on the table metadata.

### If Recovery Fails

```bash
# 1. Stop the server
systemctl stop exchangedb

# 2. Check for stale lock files
ls /var/lib/exchangedb/trades/.lock 2>/dev/null

# 3. Remove stale lock (only if server is stopped!)
rm -f /var/lib/exchangedb/trades/.lock

# 4. Start the server (WAL replay will fix consistency)
systemctl start exchangedb

# 5. Verify the table is accessible
curl -X POST http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT count(*) FROM trades"}'
```

### Prevention

- Set memory limits in Docker/Kubernetes to avoid system-level OOM kills.
- Configure `query_parallelism` to limit concurrent resource usage.
- Monitor memory via the diagnostics endpoint:
  ```bash
  curl http://localhost:9000/api/v1/diagnostics | jq '.memory'
  ```

---

## SQL Injection Prevention

**Problem:** How to safely handle user input in SQL queries.

**Related:** QuestDB issue #2025

### ExchangeDB's Approach

1. **Parameterized queries** via the PostgreSQL wire protocol's Extended
   Query Protocol. Use prepared statements with bind parameters:

   ```python
   # Python (psycopg2) -- SAFE
   cur.execute(
       "SELECT * FROM trades WHERE symbol = %s AND price > %s",
       ('BTC/USD', 50000.0)
   )
   ```

   ```go
   // Go (pgx) -- SAFE
   rows, err := conn.Query(ctx,
       "SELECT * FROM trades WHERE symbol = $1 AND price > $2",
       "BTC/USD", 50000.0)
   ```

2. **HTTP API**: The HTTP query endpoint accepts the query as a JSON string.
   Always use parameterized queries through the pgwire protocol for
   user-facing applications. If using the HTTP API, sanitize inputs at the
   application layer.

3. **Read-only by default**: The HTTP query endpoint executes all valid SQL
   including DDL. For public-facing applications, use RBAC to restrict the
   user's permissions to read-only.

### Best Practices

- Never concatenate user input into SQL strings.
- Use parameterized queries via pgwire (psycopg2, pgx, JDBC).
- Use RBAC to enforce least-privilege access.
- Enable audit logging to detect suspicious queries.

---

## Timezone and DST Issues

**Problem:** Queries return unexpected results around DST transitions or when
working with data from multiple time zones.

**Related:** QuestDB issue #4544

### ExchangeDB's Approach

1. **All timestamps are UTC internally.** Timestamps are stored as nanoseconds
   since Unix epoch (1970-01-01T00:00:00 UTC). There is no time zone offset
   stored with the data.

2. **`SAMPLE BY` uses UTC boundaries.** When using `SAMPLE BY 1d`, the day
   boundary is at midnight UTC, not local time.

3. **`ALIGN TO CALENDAR`** aligns to UTC calendar boundaries:
   ```sql
   SELECT avg(price) FROM trades SAMPLE BY 1d ALIGN TO CALENDAR;
   -- Day boundaries are at 00:00 UTC
   ```

4. **Timestamp parsing:** ISO 8601 timestamps with time zone offsets are
   converted to UTC on ingestion:
   ```sql
   -- Both store the same UTC timestamp:
   INSERT INTO t VALUES ('2024-03-10T02:00:00Z', ...);
   INSERT INTO t VALUES ('2024-03-10T03:00:00+01:00', ...);
   ```

### Common Pitfalls

**Problem:** Missing or duplicated data around DST transitions.

```sql
-- If you filter by local time, you may miss data during DST "spring forward"
-- Always filter by UTC:
SELECT * FROM trades
WHERE timestamp BETWEEN '2024-03-10T06:00:00Z' AND '2024-03-10T08:00:00Z';
```

**Problem:** `SAMPLE BY 1d` buckets don't match your local calendar day.

```sql
-- Workaround: Shift by your UTC offset
SELECT dateadd('h', -5, timestamp) AS local_time, avg(price)
FROM trades
SAMPLE BY 1d;
```

### Recommendations

- Store all timestamps in UTC.
- Convert to local time in the application layer, not in queries.
- Use `ALIGN TO CALENDAR` for UTC-aligned buckets.
- Be aware that `ALIGN TO CALENDAR WITH OFFSET` is not yet supported.

---

## LIKE Escape Characters

**Problem:** LIKE patterns with special characters (%, _) produce unexpected
matches.

**Related:** QuestDB issue #2623

### ExchangeDB's Behavior

ExchangeDB implements standard SQL LIKE with two wildcard characters:

| Character | Meaning |
|-----------|---------|
| `%` | Matches zero or more characters |
| `_` | Matches exactly one character |

### Escaping Special Characters

To match literal `%` or `_` characters, use a backslash escape:

```sql
-- Match a literal percent sign
SELECT * FROM metrics WHERE name LIKE '%\%%';

-- Match a literal underscore
SELECT * FROM metrics WHERE name LIKE 'price\_usd';
```

### Case-Insensitive LIKE

Use `ILIKE` for case-insensitive pattern matching:

```sql
SELECT * FROM trades WHERE symbol ILIKE 'btc%';
-- Matches 'BTC/USD', 'btc/usd', 'Btc/Eur', etc.
```

### Regular Expression Alternative

For complex patterns, use `regexp_match`:

```sql
SELECT * FROM trades WHERE regexp_match(symbol, '^(BTC|ETH)/USD$');
```

---

## Case Sensitivity

**Problem:** Table names, column names, or string comparisons behave
differently from expectations regarding case.

**Related:** QuestDB issue #2505

### ExchangeDB's Behavior

| Element | Case Sensitive? | Details |
|---------|----------------|---------|
| Table names | Yes | `trades` and `Trades` are different tables |
| Column names | Yes | `price` and `Price` are different columns |
| SQL keywords | No | `SELECT`, `select`, `Select` all work |
| String comparison (`=`) | Yes | `'BTC' = 'btc'` is `false` |
| LIKE | Yes | `'BTC' LIKE 'btc'` is `false` |
| ILIKE | No | `'BTC' ILIKE 'btc'` is `true` |
| SYMBOL values | Yes | Stored as-is in the symbol dictionary |

### Best Practices

- Use consistent casing for table and column names (recommend lowercase).
- Use `ILIKE` instead of `LIKE` when case-insensitive matching is needed.
- Use `lower()` or `upper()` for case-insensitive comparisons:
  ```sql
  SELECT * FROM trades WHERE lower(symbol) = 'btc/usd';
  ```
- Be consistent with SYMBOL values in ILP tags.

---

## Connection Issues

### psql Connection Refused

**Error:** `connection refused` or `could not connect to server`

**Solutions:**

1. Verify the server is running:
   ```bash
   curl http://localhost:9000/health
   ```
2. Check the pgwire port is correct (default 8812, not PostgreSQL's 5432):
   ```bash
   psql -h localhost -p 8812
   ```
3. Check bind address -- if bound to `127.0.0.1`, remote connections are rejected:
   ```toml
   [pgwire]
   bind = "0.0.0.0:8812"  # Accept from all interfaces
   ```
4. Check firewall rules:
   ```bash
   nc -zv localhost 8812
   ```

### Grafana Connection Issues

**Problem:** Grafana cannot connect to ExchangeDB.

**Solution:**

1. Add a PostgreSQL data source in Grafana.
2. Host: `exchangedb-host` (or Docker container name).
3. Port: `8812` (not 5432).
4. Database: `exchangedb`.
5. SSL Mode: `disable` (unless TLS is configured).
6. User/Password: leave empty (unless auth is enabled).
7. In "PostgreSQL Details", set version to "15" or higher.

### DBeaver Connection Issues

**Problem:** DBeaver shows errors or missing features.

**Solution:**

1. Create a PostgreSQL connection (not a custom driver).
2. Host: `localhost`, Port: `8812`.
3. Database: `exchangedb`.
4. Authentication: leave empty or use configured credentials.
5. On the "Driver properties" tab:
   - Set `prepareThreshold` to `0` to avoid prepared statement issues.
   - Set `preferQueryMode` to `simple`.

### Connection Timeout

**Problem:** Connections time out during long queries.

**Solution:**

- Increase client-side timeout settings.
- For psql: `PGCONNECT_TIMEOUT=30 psql -h localhost -p 8812`
- For HTTP: Increase the curl timeout: `curl --max-time 300 ...`
- Check if the query itself is slow: use `EXPLAIN ANALYZE`.

---

## Performance Tuning

### Slow Queries

**Diagnosis:**

```sql
EXPLAIN ANALYZE SELECT ...;
```

Check for:

1. **Full table scan**: Look for `scan` cursor without partition pruning.
   - Fix: Add timestamp filter to enable partition pruning.
2. **Missing SYMBOL filter**: SYMBOL columns have bitmap indexes.
   - Fix: Filter on SYMBOL columns when possible.
3. **Large result set**: LIMIT pushdown may not be applied.
   - Fix: Add `LIMIT` to your query.
4. **GROUP BY on high-cardinality column**: Creates many groups.
   - Fix: Filter first, then aggregate.

**Common optimizations:**

```sql
-- SLOW: Full scan, no partition pruning
SELECT avg(price) FROM trades;

-- FAST: Partition pruning (only reads March partitions)
SELECT avg(price) FROM trades
WHERE timestamp BETWEEN '2024-03-01' AND '2024-03-31';

-- SLOW: Full scan with string comparison
SELECT * FROM trades WHERE cast(symbol AS VARCHAR) = 'BTC/USD';

-- FAST: SYMBOL index lookup
SELECT * FROM trades WHERE symbol = 'BTC/USD';
```

### Write Performance

| Scenario | Recommendation |
|----------|---------------|
| Maximum throughput | Use ILP/TCP with batching, `async` commit mode |
| Maximum durability | Use `sync` commit mode |
| Bulk historical load | Use CSV import or COPY FROM |
| Many small inserts | Batch into multi-row INSERT statements |

### Query Parallelism

```toml
[performance]
query_parallelism = 0   # Auto (num_cpus)
```

For dedicated query servers, set this to the number of CPU cores. For mixed
read/write workloads, consider setting it to half the CPU cores.

---

## WAL Issues

### WAL Segments Accumulating

**Problem:** The `_wal` directory grows continuously.

**Cause:** WAL merge job is not keeping up with write throughput.

**Solutions:**

1. Run VACUUM to clean up applied segments:
   ```sql
   VACUUM trades;
   ```
2. Check WAL status:
   ```bash
   curl http://localhost:9000/admin/wal
   ```
3. Increase WAL segment size to reduce rotation frequency:
   ```toml
   [storage]
   wal_max_segment_size = "256MB"
   ```

### WAL Replay Failure on Startup

**Problem:** Server fails to start due to corrupted WAL segment.

**Solutions:**

1. Check the server logs for the specific WAL error.
2. If a segment is corrupted:
   ```bash
   # Stop the server
   # Move the corrupted segment
   mv /data/exchangedb/trades/_wal/wal-N /tmp/wal-N-corrupt
   # Start the server (data in that segment is lost)
   ```
3. Restore from backup if data integrity is critical.

### WAL Disk Space

WAL segments are retained until:

- They have been merged into the column store AND
- They are no longer needed for PITR AND
- VACUUM has been run.

Monitor WAL disk usage:

```bash
du -sh /data/exchangedb/*/_wal/
```

---

## Disk Space Management

### Running Out of Disk Space

**Immediate actions:**

1. Run VACUUM on all tables:
   ```sql
   VACUUM;
   ```
2. Drop old data:
   ```sql
   ALTER TABLE trades DETACH PARTITION '2024-01-01';
   -- Delete the detached partition directory:
   -- rm -rf /data/exchangedb/trades/2024-01-01.detached
   ```
3. Compress and archive detached partitions:
   ```bash
   tar czf /archive/trades-2024-01-01.tar.gz \
     /data/exchangedb/trades/2024-01-01.detached
   rm -rf /data/exchangedb/trades/2024-01-01.detached
   ```

### Monitoring Disk Space

```bash
# Check total data directory size
du -sh /data/exchangedb/

# Check per-table size
du -sh /data/exchangedb/*/

# Check free disk space
df -h /data/

# Via diagnostics endpoint
curl http://localhost:9000/api/v1/diagnostics | jq '.storage.disk_free_bytes'
```

### Preventing Disk Space Issues

1. Enable retention policies:
   ```toml
   [retention]
   enabled = true
   max_age = "90d"
   ```
2. Schedule regular VACUUM jobs.
3. Set up disk space monitoring alerts.
4. Use tiered storage for older data.

---

## Common SQL Errors

### "table not found"

```
error: table 'traeds' not found (SQLSTATE 42P01)
```

**Fix:** Check table name spelling. Table names are case-sensitive.

```sql
SHOW TABLES;  -- List all tables
```

### "column not found"

```
error: column 'proce' not found (SQLSTATE 42703)
```

**Fix:** Check column names. Use DESCRIBE to see the schema:

```sql
DESCRIBE trades;
```

### "table already exists"

```
error: table 'trades' already exists (SQLSTATE 42P07)
```

**Fix:** Use `IF NOT EXISTS`:

```sql
CREATE TABLE IF NOT EXISTS trades (...);
```

### "syntax error"

```
error: syntax error at position 42 (SQLSTATE 42601)
```

**Fix:** Check SQL syntax. Common causes:

- Missing commas between columns.
- Unquoted reserved words used as identifiers.
- Missing parentheses.
- Wrong quote characters (use single quotes for strings, not double quotes).

### "data type mismatch"

```
error: data type mismatch (SQLSTATE 42804)
```

**Fix:** Use explicit CAST:

```sql
INSERT INTO trades VALUES (CAST('2024-03-01' AS TIMESTAMP), 'BTC/USD', 65000.0, 1.5, 'buy');
```

---

## Data Type Issues

### Timestamp Precision

**Problem:** Timestamps lose precision when inserted.

ExchangeDB stores timestamps in nanoseconds. Make sure your input includes
sufficient precision:

```sql
-- Full nanosecond precision
INSERT INTO t VALUES ('2024-03-01T10:00:00.123456789Z', ...);

-- Millisecond precision (nanos are zero-padded)
INSERT INTO t VALUES ('2024-03-01T10:00:00.123Z', ...);
```

### SYMBOL vs VARCHAR

**Problem:** Queries on string columns are slow.

**Fix:** Use SYMBOL for low-cardinality string columns. SYMBOL columns have
bitmap indexes and are dictionary-encoded:

```sql
-- SLOW: VARCHAR column, requires full scan
CREATE TABLE t (symbol VARCHAR, ...);

-- FAST: SYMBOL column, uses bitmap index
CREATE TABLE t (symbol SYMBOL, ...);
```

### Floating-Point Precision

**Problem:** Aggregations produce slightly different results from expected.

ExchangeDB uses IEEE 754 double-precision floating point, which has inherent
precision limitations. For numerically stable summation:

```sql
-- Standard sum (may accumulate rounding errors on large datasets)
SELECT sum(price * volume) FROM trades;

-- Kahan compensated sum (more accurate)
SELECT ksum(price * volume) FROM trades;

-- Neumaier compensated sum (most accurate)
SELECT nsum(price * volume) FROM trades;
```

---

## Ingestion Issues

### ILP Lines Rejected

**Problem:** ILP ingestion silently drops lines.

**Diagnosis:**

1. Check server logs for parse errors.
2. Use ILP over HTTP to get error responses:
   ```bash
   curl -X POST http://localhost:9000/api/v1/write \
     -d 'invalid line format'
   ```
3. Common ILP format errors:
   - Missing field after the space separator.
   - Spaces in tag values (not supported).
   - Missing newline at end of last line.

### Auto-Created Table Has Wrong Schema

**Problem:** An ILP-created table has unexpected column types.

**Fix:** The schema is inferred from the first line. If a field value looks
like a float on the first line but is actually an integer:

```
# This creates 'count' as DOUBLE (no 'i' suffix)
metrics,host=srv1 count=42

# This creates 'count' as LONG
metrics,host=srv1 count=42i
```

To fix: drop and re-create the table with the correct schema, then re-ingest.

### Slow ILP Ingestion

**Solutions:**

1. Increase batch size:
   ```toml
   [ilp]
   batch_size = 10000
   ```
2. Use persistent TCP connections (avoid reconnecting per batch).
3. Buffer lines client-side and send in large batches.
4. Use multiple TCP connections for parallel ingestion.

---

## Memory Issues

### High Memory Usage

**Diagnosis:**

```bash
curl http://localhost:9000/api/v1/diagnostics | jq '.memory'
```

**Solutions:**

1. Reduce `query_parallelism` to limit concurrent memory usage.
2. Queries that sort or GROUP BY large datasets spill to disk automatically.
   Ensure sufficient disk space for spill files.
3. Limit result set sizes with `LIMIT`.
4. Reduce WAL segment size to limit WAL memory overhead.

### OOM Kill

**Prevention:**

- Set appropriate memory limits in Docker/Kubernetes.
- Monitor RSS via Prometheus metrics.
- Enable swap as a safety net (performance will degrade but the process
  survives).

**Recovery:** See [Table Locked After OOM](#table-locked-after-oom).

---

## Startup Problems

### "address already in use"

**Problem:** Server fails to start because a port is already bound.

```
error: address already in use: 0.0.0.0:9000
```

**Fix:**

```bash
# Find the process using the port
lsof -i :9000

# Kill it or use a different port
exchange-db server --bind 0.0.0.0:9001
```

### "permission denied" on Data Directory

**Problem:** Server cannot read or write the data directory.

```bash
# Check ownership
ls -la /var/lib/exchangedb/

# Fix permissions
chown -R exchangedb:exchangedb /var/lib/exchangedb/
chmod 750 /var/lib/exchangedb/
```

### Corrupted Metadata

**Problem:** Server fails to start due to corrupted `_meta` or `_txn` files.

**Recovery:**

1. Check server logs for the specific corruption error.
2. If WAL is intact, the data can be recovered:
   ```bash
   # Backup the corrupted metadata
   mv /data/exchangedb/trades/_meta /data/exchangedb/trades/_meta.corrupt
   # The server will attempt to rebuild metadata from WAL on startup
   ```
3. As a last resort, restore from a backup.

---

## Getting Help

### Diagnostic Information to Collect

When reporting issues, include:

1. ExchangeDB version: `exchange-db --version`
2. OS and architecture: `uname -a`
3. Diagnostics output: `curl http://localhost:9000/api/v1/diagnostics`
4. Relevant server logs.
5. The SQL query causing the issue.
6. EXPLAIN ANALYZE output for query issues.
7. Table schema: `SHOW CREATE TABLE <table_name>`
