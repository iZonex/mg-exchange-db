# ExchangeDB SQL Reference

Complete SQL reference for ExchangeDB. ExchangeDB implements a comprehensive SQL
dialect compatible with PostgreSQL clients and extended with QuestDB-style
time-series operations.

---

## Table of Contents

- [Data Definition Language (DDL)](#data-definition-language-ddl)
- [Data Manipulation Language (DML)](#data-manipulation-language-dml)
- [Queries](#queries)
- [Joins](#joins)
- [Time-Series Extensions](#time-series-extensions)
- [Window Functions](#window-functions)
- [Set Operations](#set-operations)
- [Expressions and Operators](#expressions-and-operators)
- [Administrative Statements](#administrative-statements)
- [Transaction Control](#transaction-control)

---

## Data Definition Language (DDL)

### CREATE TABLE

Creates a new table.

**Syntax:**

```
CREATE TABLE [IF NOT EXISTS] table_name (
    column_name data_type [, ...]
) [TIMESTAMP(column_name)] [PARTITION BY {NONE | HOUR | DAY | WEEK | MONTH | YEAR}];
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `IF NOT EXISTS` | Suppresses error if table already exists |
| `table_name` | Name of the table to create |
| `column_name` | Name of a column |
| `data_type` | Column data type (see [Data Types](DATA_TYPES.md)) |
| `TIMESTAMP(col)` | Designates the time column for partitioning and time-series ops |
| `PARTITION BY` | Partitioning strategy for time-based data |

**Examples:**

```sql
-- Basic table with designated timestamp and daily partitions
CREATE TABLE trades (
    timestamp TIMESTAMP,
    symbol    SYMBOL,
    price     DOUBLE,
    volume    DOUBLE,
    side      SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- Table without partitioning
CREATE TABLE settings (
    key   VARCHAR,
    value VARCHAR
);

-- Idempotent creation
CREATE TABLE IF NOT EXISTS trades (
    timestamp TIMESTAMP,
    symbol    SYMBOL,
    price     DOUBLE
) TIMESTAMP(timestamp) PARTITION BY HOUR;
```

**Notes:**

- Partition strategies: `NONE` (no partitioning), `HOUR`, `DAY`, `WEEK`,
  `MONTH`, `YEAR`.
- Tables with a designated timestamp are required for `SAMPLE BY`, `LATEST ON`,
  and `ASOF JOIN`.
- SYMBOL columns are automatically indexed with a bitmap index.

---

### ALTER TABLE

Modifies an existing table's schema or partitions.

**Syntax:**

```
ALTER TABLE table_name
    ADD COLUMN column_name data_type
  | DROP COLUMN column_name
  | RENAME COLUMN old_name TO new_name
  | SET TYPE column_name new_type
  | DETACH PARTITION 'partition_name'
  | ATTACH PARTITION 'partition_name'
  | SQUASH PARTITIONS 'start' 'end';
```

**Examples:**

```sql
-- Add a column
ALTER TABLE trades ADD COLUMN fee DOUBLE;

-- Drop a column
ALTER TABLE trades DROP COLUMN fee;

-- Rename a column
ALTER TABLE trades RENAME COLUMN side TO direction;

-- Change column type (widening only)
ALTER TABLE trades SET TYPE volume FLOAT;

-- Detach a partition (moves to detached state, not queryable)
ALTER TABLE trades DETACH PARTITION '2024-01-01';

-- Attach a previously detached partition
ALTER TABLE trades ATTACH PARTITION '2024-01-01';

-- Merge two adjacent partitions into one
ALTER TABLE trades SQUASH PARTITIONS '2024-01-01' '2024-01-02';
```

**Notes:**

- `SET TYPE` supports widening casts only (e.g., `INT` to `LONG`, `FLOAT` to
  `DOUBLE`).
- Detached partitions are moved to a detached directory and can be re-attached.
- `SQUASH PARTITIONS` merges data from two adjacent partitions.

---

### DROP TABLE

Removes a table and all its data.

**Syntax:**

```
DROP TABLE [IF EXISTS] table_name;
```

**Examples:**

```sql
DROP TABLE trades;
DROP TABLE IF EXISTS old_trades;
```

---

### TRUNCATE TABLE

Removes all rows from a table without dropping the table structure.

**Syntax:**

```
TRUNCATE TABLE table_name;
```

**Examples:**

```sql
TRUNCATE TABLE trades;
```

**Notes:**

- Faster than `DELETE FROM table_name` because it drops and re-creates
  partition files.
- Non-transactional -- cannot be rolled back.

---

### CREATE INDEX / DROP INDEX

ExchangeDB automatically creates bitmap indexes on SYMBOL columns. Explicit
index management SQL is not currently supported. The engine builds and
maintains indexes internally.

**Note:** SYMBOL columns are always indexed. No manual index creation is needed.

---

### CREATE SEQUENCE

Creates an auto-incrementing sequence.

**Syntax:**

```
CREATE SEQUENCE [IF NOT EXISTS] sequence_name
    [START WITH n]
    [INCREMENT BY n]
    [MINVALUE n]
    [MAXVALUE n]
    [CACHE n];
```

**Examples:**

```sql
CREATE SEQUENCE trade_id_seq START WITH 1 INCREMENT BY 1;

-- Use in INSERT
INSERT INTO trades (id, timestamp, symbol, price)
VALUES (nextval('trade_id_seq'), now(), 'BTC/USD', 65000.0);

-- Get current value
SELECT currval('trade_id_seq');

-- Reset value
SELECT setval('trade_id_seq', 1000);
```

---

### DROP SEQUENCE

```sql
DROP SEQUENCE trade_id_seq;
DROP SEQUENCE IF EXISTS trade_id_seq;
```

---

### CREATE VIEW

Views are not currently implemented. Use CTEs (Common Table Expressions) as
an alternative:

```sql
WITH my_view AS (
    SELECT symbol, avg(price) AS avg_price
    FROM trades
    GROUP BY symbol
)
SELECT * FROM my_view WHERE avg_price > 50000;
```

---

### CREATE MATERIALIZED VIEW

Creates a precomputed view that stores results physically.

**Syntax:**

```
CREATE MATERIALIZED VIEW view_name AS select_statement;
```

**Examples:**

```sql
CREATE MATERIALIZED VIEW ohlcv_1h AS
SELECT symbol,
    first(price)  AS open,
    max(price)    AS high,
    min(price)    AS low,
    last(price)   AS close,
    sum(volume)   AS volume
FROM trades
SAMPLE BY 1h;
```

**Refresh:**

```sql
REFRESH MATERIALIZED VIEW ohlcv_1h;
```

**Drop:**

```sql
DROP MATERIALIZED VIEW ohlcv_1h;
```

---

### CREATE TRIGGER

Triggers are not currently implemented in ExchangeDB.

---

### COMMENT ON

Comments on database objects are not currently implemented.

---

## Data Manipulation Language (DML)

### INSERT

Inserts one or more rows into a table.

**Syntax:**

```
INSERT INTO table_name [(column_list)]
VALUES (value_list) [, (value_list) ...];

INSERT INTO table_name [(column_list)]
SELECT ...;
```

**Examples:**

```sql
-- Single row
INSERT INTO trades VALUES (
    '2024-03-01T10:00:00Z', 'BTC/USD', 65000.0, 1.5, 'buy'
);

-- Named columns
INSERT INTO trades (timestamp, symbol, price)
VALUES ('2024-03-01T10:00:00Z', 'BTC/USD', 65000.0);

-- Multiple rows
INSERT INTO trades VALUES
    ('2024-03-01T10:00:00Z', 'BTC/USD', 65000.0, 1.5, 'buy'),
    ('2024-03-01T10:00:01Z', 'ETH/USD', 3400.0, 10.0, 'sell');

-- INSERT from SELECT
INSERT INTO trades_archive
SELECT * FROM trades WHERE timestamp < '2024-01-01';
```

---

### INSERT ON CONFLICT (Upsert)

Performs an upsert: insert if no conflict, update if a matching row exists.

**Syntax:**

```
INSERT INTO table_name VALUES (...)
ON CONFLICT DO UPDATE SET column = EXCLUDED.column [, ...];

INSERT INTO table_name VALUES (...)
ON CONFLICT DO NOTHING;
```

**Examples:**

```sql
-- Update price if a matching row exists
INSERT INTO trades VALUES ('2024-03-01T10:00:00Z', 'BTC/USD', 65100.0, 1.5, 'buy')
ON CONFLICT DO UPDATE SET price = EXCLUDED.price;

-- Skip if conflict
INSERT INTO trades VALUES ('2024-03-01T10:00:00Z', 'BTC/USD', 65100.0, 1.5, 'buy')
ON CONFLICT DO NOTHING;
```

---

### UPDATE

Updates existing rows that match a condition.

**Syntax:**

```
UPDATE table_name
SET column = expression [, ...]
[WHERE condition];
```

**Examples:**

```sql
UPDATE trades SET price = 65100.0
WHERE symbol = 'BTC/USD' AND timestamp = '2024-03-01T10:00:00Z';

UPDATE trades SET volume = volume * 1.1
WHERE symbol = 'ETH/USD';
```

**Notes:**

- UPDATE requires WAL to be enabled.
- UPDATE with JOIN is not yet supported.

---

### DELETE

Deletes rows matching a condition.

**Syntax:**

```
DELETE FROM table_name [WHERE condition];
```

**Examples:**

```sql
DELETE FROM trades WHERE timestamp < '2024-01-01';
DELETE FROM trades WHERE symbol = 'DELIST/USD';
```

---

### MERGE

Performs conditional insert/update based on whether rows match.

**Syntax:**

```
MERGE INTO target_table t
USING source_table s ON join_condition
WHEN MATCHED THEN UPDATE SET column = expression [, ...]
WHEN NOT MATCHED THEN INSERT VALUES (value_list);
```

**Examples:**

```sql
MERGE INTO trades t
USING new_trades n ON t.timestamp = n.timestamp AND t.symbol = n.symbol
WHEN MATCHED THEN UPDATE SET price = n.price, volume = n.volume
WHEN NOT MATCHED THEN INSERT VALUES (n.timestamp, n.symbol, n.price, n.volume, n.side);
```

---

### COPY TO / COPY FROM

Imports or exports data in CSV, TSV, or Parquet format.

**Syntax:**

```
COPY table_name TO 'path' WITH (FORMAT {CSV|TSV|PARQUET} [, HEADER {TRUE|FALSE}]);
COPY table_name FROM 'path' WITH (FORMAT {CSV|TSV|PARQUET} [, HEADER {TRUE|FALSE}]);
```

**Examples:**

```sql
-- Export to CSV
COPY trades TO '/tmp/trades.csv' WITH (FORMAT CSV, HEADER TRUE);

-- Import from CSV
COPY trades FROM '/tmp/trades.csv' WITH (FORMAT CSV, HEADER TRUE);

-- Export to Parquet
COPY trades TO '/tmp/trades.parquet' WITH (FORMAT PARQUET);
```

---

## Queries

### SELECT

**Syntax:**

```
SELECT [DISTINCT] select_list
FROM table_reference
[WHERE condition]
[GROUP BY expression_list]
[HAVING condition]
[ORDER BY expression_list [ASC|DESC]]
[LIMIT count]
[OFFSET skip];
```

**Examples:**

```sql
-- All columns
SELECT * FROM trades;

-- Selected columns with alias
SELECT symbol, price * volume AS notional FROM trades;

-- Filtering
SELECT * FROM trades
WHERE timestamp BETWEEN '2024-01-01' AND '2024-03-01'
  AND symbol IN ('BTC/USD', 'ETH/USD')
  AND price > 50000;

-- Ordering and pagination
SELECT * FROM trades
ORDER BY timestamp DESC
LIMIT 100 OFFSET 50;

-- DISTINCT
SELECT DISTINCT symbol FROM trades;

-- SELECT without FROM
SELECT now(), version(), current_database();
```

---

### WHERE

The WHERE clause supports all standard SQL predicates:

| Predicate | Example |
|-----------|---------|
| Comparison | `price > 50000` |
| BETWEEN | `price BETWEEN 60000 AND 70000` |
| IN | `symbol IN ('BTC/USD', 'ETH/USD')` |
| LIKE | `symbol LIKE 'BTC%'` |
| ILIKE | `symbol ILIKE 'btc%'` |
| IS NULL | `fee IS NULL` |
| IS NOT NULL | `fee IS NOT NULL` |
| EXISTS | `EXISTS (SELECT 1 FROM ...)` |
| NOT | `NOT (price > 50000)` |
| AND / OR | `price > 50000 AND volume > 1.0` |

---

### GROUP BY

**Standard aggregation:**

```sql
SELECT symbol, count(*), avg(price), sum(volume)
FROM trades
GROUP BY symbol
HAVING count(*) > 1000
ORDER BY sum(volume) DESC;
```

**GROUPING SETS:**

```sql
SELECT symbol, side, count(*)
FROM trades
GROUP BY GROUPING SETS ((symbol), (side), (symbol, side), ());
```

**ROLLUP:**

```sql
SELECT symbol, extract(hour FROM timestamp) AS hour, avg(price)
FROM trades
GROUP BY ROLLUP(symbol, extract(hour FROM timestamp));
```

**CUBE:**

```sql
SELECT symbol, side, count(*)
FROM trades
GROUP BY CUBE(symbol, side);
```

---

### HAVING

Filters groups after aggregation:

```sql
SELECT symbol, avg(price) AS avg_price
FROM trades
GROUP BY symbol
HAVING avg(price) > 50000;
```

---

### ORDER BY

```sql
SELECT * FROM trades ORDER BY timestamp DESC;
SELECT * FROM trades ORDER BY symbol ASC, price DESC;
SELECT symbol, sum(volume) AS vol FROM trades GROUP BY symbol ORDER BY vol DESC;
```

---

### LIMIT and OFFSET

```sql
SELECT * FROM trades LIMIT 100;
SELECT * FROM trades LIMIT 100 OFFSET 200;
```

---

### DISTINCT

```sql
SELECT DISTINCT symbol FROM trades;
SELECT DISTINCT symbol, side FROM trades;
```

**Note:** `DISTINCT ON (column)` is not currently supported.

---

### Subqueries

**Scalar subquery:**

```sql
SELECT *, (SELECT avg(price) FROM trades) AS market_avg
FROM trades;
```

**IN subquery:**

```sql
SELECT * FROM trades
WHERE symbol IN (SELECT symbol FROM watchlist);
```

**EXISTS subquery:**

```sql
SELECT DISTINCT symbol FROM trades t
WHERE EXISTS (SELECT 1 FROM quotes q WHERE q.symbol = t.symbol);
```

**Correlated subquery:**

```sql
SELECT * FROM trades t
WHERE price > (SELECT avg(price) FROM trades WHERE symbol = t.symbol);
```

---

### Common Table Expressions (CTEs)

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

**Recursive CTEs:**

```sql
WITH RECURSIVE seq AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 100
)
SELECT n FROM seq;
```

---

### CASE WHEN

```sql
SELECT symbol, price,
    CASE
        WHEN price > 60000 THEN 'high'
        WHEN price > 40000 THEN 'medium'
        ELSE 'low'
    END AS tier
FROM trades;

-- Simple CASE
SELECT symbol,
    CASE side
        WHEN 'buy'  THEN 'B'
        WHEN 'sell' THEN 'S'
        ELSE '?'
    END AS direction
FROM trades;
```

---

### CAST

```sql
SELECT CAST(price AS INT) FROM trades;
SELECT CAST('2024-03-01' AS TIMESTAMP);
SELECT price::INT FROM trades;      -- PostgreSQL double-colon syntax
```

---

### PIVOT

Transforms rows into columns.

```sql
SELECT * FROM (
    SELECT symbol, side, volume FROM trades
)
PIVOT (sum(volume) FOR side IN ('buy', 'sell'));
```

---

### EXPLAIN / EXPLAIN ANALYZE

Shows the query execution plan.

```sql
EXPLAIN SELECT * FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h;

EXPLAIN ANALYZE SELECT symbol, avg(price) FROM trades GROUP BY symbol;
```

`EXPLAIN ANALYZE` actually runs the query and includes execution timing for
each stage.

---

## Joins

ExchangeDB supports 10 join types:

### INNER JOIN

```sql
SELECT t.symbol, t.price, q.bid, q.ask
FROM trades t
INNER JOIN quotes q ON t.symbol = q.symbol AND t.timestamp = q.timestamp;
```

### LEFT JOIN

```sql
SELECT t.*, q.bid, q.ask
FROM trades t
LEFT JOIN quotes q ON t.symbol = q.symbol;
```

### RIGHT JOIN

```sql
SELECT t.*, q.bid, q.ask
FROM trades t
RIGHT JOIN quotes q ON t.symbol = q.symbol;
```

### FULL OUTER JOIN

```sql
SELECT *
FROM trades t
FULL OUTER JOIN quotes q ON t.symbol = q.symbol;
```

### CROSS JOIN

```sql
SELECT * FROM symbols CROSS JOIN timeframes;
```

### ASOF JOIN

Temporal point-in-time join. Matches each row in the left table with the most
recent row in the right table at or before the left row's timestamp.

```sql
SELECT t.timestamp, t.symbol, t.price, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q ON (t.symbol = q.symbol);
```

**Notes:**

- Both tables must have a designated timestamp column.
- The right table's timestamp must be less than or equal to the left table's.
- Extremely useful for point-in-time financial analysis.

### LATERAL JOIN

```sql
SELECT t.symbol, l.*
FROM (SELECT DISTINCT symbol FROM trades) t,
LATERAL (
    SELECT * FROM trades
    WHERE symbol = t.symbol
    ORDER BY timestamp DESC
    LIMIT 5
) l;
```

### SEMI JOIN

Returns rows from the left table that have a match in the right table:

```sql
SELECT * FROM trades t
WHERE EXISTS (SELECT 1 FROM watchlist w WHERE w.symbol = t.symbol);
```

### ANTI JOIN

Returns rows from the left table that have no match in the right table:

```sql
SELECT * FROM trades t
WHERE NOT EXISTS (SELECT 1 FROM blacklist b WHERE b.symbol = t.symbol);
```

### BAND JOIN / MARK JOIN

Internal join types used by the optimizer. BAND JOIN matches rows within
a value range. MARK JOIN is used for semi/anti join optimization.

---

## Time-Series Extensions

### SAMPLE BY

Aggregates data into fixed time intervals (time bucketing).

**Syntax:**

```
SELECT aggregate_list FROM table
[WHERE condition]
SAMPLE BY interval
[FILL ({NONE | NULL | PREV | LINEAR | constant})]
[ALIGN TO {CALENDAR | FIRST OBSERVATION}];
```

**Interval format:** A number followed by a unit:

| Unit | Meaning | Example |
|------|---------|---------|
| `s` | seconds | `SAMPLE BY 30s` |
| `m` | minutes | `SAMPLE BY 5m` |
| `h` | hours | `SAMPLE BY 1h` |
| `d` | days | `SAMPLE BY 1d` |
| `w` | weeks | `SAMPLE BY 1w` |
| `M` | months | `SAMPLE BY 1M` |
| `y` | years | `SAMPLE BY 1y` |

**Examples:**

```sql
-- 1-hour OHLCV bars
SELECT symbol,
    first(price)  AS open,
    max(price)    AS high,
    min(price)    AS low,
    last(price)   AS close,
    sum(volume)   AS volume
FROM trades
WHERE symbol = 'BTC/USD'
SAMPLE BY 1h;

-- 5-minute bars with forward-fill for gaps
SELECT symbol, avg(price), sum(volume)
FROM trades
SAMPLE BY 5m FILL(PREV);

-- Linear interpolation for missing intervals
SELECT avg(price) FROM trades SAMPLE BY 1h FILL(LINEAR);

-- Fill with constant value
SELECT avg(price), sum(volume) FROM trades SAMPLE BY 1d FILL(0);

-- Calendar-aligned buckets
SELECT avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR;

-- Align to first observation
SELECT avg(price) FROM trades SAMPLE BY 1h ALIGN TO FIRST OBSERVATION;
```

**FILL modes:**

| Mode | Behavior |
|------|----------|
| `NONE` | Omit intervals with no data (default) |
| `NULL` | Include intervals with NULL values |
| `PREV` | Forward-fill with previous interval's values |
| `LINEAR` | Linear interpolation between known values |
| `constant` | Fill with a literal value (e.g., `FILL(0)`) |

---

### LATEST ON

Returns the most recent row for each partition key.

**Syntax:**

```
SELECT column_list FROM table
LATEST ON timestamp_column PARTITION BY partition_column [, ...];
```

**Examples:**

```sql
-- Latest trade per symbol
SELECT * FROM trades
LATEST ON timestamp PARTITION BY symbol;

-- Latest trade per symbol and side
SELECT * FROM trades
LATEST ON timestamp PARTITION BY symbol, side;
```

**Notes:**

- Requires a designated timestamp column.
- Uses an optimized execution path that avoids full table scan when possible.
- Equivalent to `SELECT DISTINCT ON (symbol) ... ORDER BY timestamp DESC` in
  PostgreSQL, but significantly faster.

---

## Window Functions

**Syntax:**

```
function_name() OVER (
    [PARTITION BY column_list]
    [ORDER BY column_list [ASC|DESC]]
    [frame_clause]
)
```

**Frame clause:**

```
{ROWS | RANGE} BETWEEN
    {UNBOUNDED PRECEDING | n PRECEDING | CURRENT ROW}
    AND
    {CURRENT ROW | n FOLLOWING | UNBOUNDED FOLLOWING}
```

### Ranking Functions

```sql
SELECT symbol, timestamp, price,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp) AS rn,
    RANK() OVER (PARTITION BY symbol ORDER BY price DESC) AS price_rank,
    DENSE_RANK() OVER (PARTITION BY symbol ORDER BY price DESC) AS dense_rank,
    NTILE(4) OVER (ORDER BY price) AS quartile
FROM trades;
```

### Offset Functions

```sql
SELECT symbol, timestamp, price,
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev_price,
    LEAD(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) AS next_price,
    FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS first_price,
    LAST_VALUE(price) OVER (PARTITION BY symbol ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price,
    NTH_VALUE(price, 3) OVER (PARTITION BY symbol ORDER BY timestamp) AS third_price
FROM trades;
```

### Running Aggregates

```sql
SELECT symbol, timestamp, price,
    AVG(price) OVER (PARTITION BY symbol ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma10,
    SUM(volume) OVER (PARTITION BY symbol ORDER BY timestamp) AS cumulative_vol,
    MIN(price) OVER (PARTITION BY symbol ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min,
    MAX(price) OVER (PARTITION BY symbol ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max,
    COUNT(*) OVER (PARTITION BY symbol) AS total_trades
FROM trades;
```

### Distribution Functions

```sql
SELECT symbol, price,
    PERCENT_RANK() OVER (ORDER BY price) AS pct_rank,
    CUME_DIST() OVER (ORDER BY price) AS cume_dist
FROM trades;
```

---

## Set Operations

### UNION / UNION ALL

```sql
SELECT symbol FROM exchange_a
UNION ALL
SELECT symbol FROM exchange_b;

SELECT symbol FROM exchange_a
UNION
SELECT symbol FROM exchange_b;  -- removes duplicates
```

### INTERSECT

```sql
SELECT symbol FROM exchange_a
INTERSECT
SELECT symbol FROM exchange_b;
```

### EXCEPT

```sql
SELECT symbol FROM all_symbols
EXCEPT
SELECT symbol FROM delisted;
```

---

## Expressions and Operators

### Arithmetic

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `price + fee` |
| `-` | Subtraction | `price - cost` |
| `*` | Multiplication | `price * volume` |
| `/` | Division | `total / count` |
| `%` | Modulo | `id % 10` |

### Comparison

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `!=`, `<>` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |

### Logical

| Operator | Description |
|----------|-------------|
| `AND` | Logical AND |
| `OR` | Logical OR |
| `NOT` | Logical NOT |

### BETWEEN

```sql
SELECT * FROM trades WHERE price BETWEEN 60000 AND 70000;
SELECT * FROM trades WHERE timestamp BETWEEN '2024-01-01' AND '2024-03-01';
```

### IN

```sql
SELECT * FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD', 'SOL/USD');
```

### LIKE / ILIKE

```sql
SELECT * FROM trades WHERE symbol LIKE 'BTC%';
SELECT * FROM trades WHERE symbol ILIKE 'btc%';  -- case-insensitive
SELECT * FROM trades WHERE symbol LIKE '%USD';
SELECT * FROM trades WHERE symbol LIKE 'BTC_USD'; -- single character
```

### IS NULL / IS NOT NULL

```sql
SELECT * FROM trades WHERE fee IS NULL;
SELECT * FROM trades WHERE fee IS NOT NULL;
```

---

## Administrative Statements

### VACUUM

Reclaims disk space from applied WAL segments and deleted data.

```sql
VACUUM;
VACUUM trades;
```

### SHOW

```sql
SHOW TABLES;
SHOW COLUMNS FROM trades;
SHOW CREATE TABLE trades;
```

### DESCRIBE

```sql
DESCRIBE trades;
-- Equivalent to SHOW COLUMNS FROM trades
```

### SET / SHOW Variable

Compatibility stubs for PostgreSQL clients:

```sql
SET client_encoding TO 'UTF8';
SET timezone TO 'UTC';
SHOW timezone;
SHOW server_version;
```

### GRANT / REVOKE

```sql
-- Create user and role
CREATE USER analyst WITH PASSWORD 'secret';
CREATE ROLE readonly;

-- Grant permissions
GRANT READ ON trades TO readonly;
GRANT readonly TO analyst;

-- Revoke
REVOKE READ ON trades FROM readonly;

-- Drop
DROP USER analyst;
DROP ROLE readonly;
```

**Permission types:**

| Permission | Description |
|------------|-------------|
| `READ` | Query data from tables |
| `WRITE` | Insert/update/delete data |
| `DDL` | Create, alter, drop tables |
| `ADMIN` | Full administrative access |
| `SYSTEM` | System-level operations |

---

## Transaction Control

ExchangeDB provides transaction statement compatibility for PostgreSQL clients.
These are currently implemented as no-ops since ExchangeDB uses WAL-based
consistency rather than traditional ACID transactions.

```sql
BEGIN;
-- statements
COMMIT;

-- or
BEGIN;
-- statements
ROLLBACK;
```

**Note:** Each statement is individually consistent. Multi-statement transactions
with rollback semantics are not yet supported.

---

## Virtual Tables and Table Functions

### long_sequence()

Generates a sequence of rows, useful for testing:

```sql
SELECT x, rnd_double() * 100 AS price
FROM long_sequence(1000000);
```

### generate_series()

PostgreSQL-compatible series generator:

```sql
SELECT * FROM generate_series(1, 100);
SELECT * FROM generate_series('2024-01-01'::TIMESTAMP, '2024-12-31'::TIMESTAMP, '1 day');
```

### VALUES

Inline row constructor:

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name);
```

### read_parquet()

Read data from a Parquet file:

```sql
SELECT * FROM read_parquet('/path/to/data.parquet');
```
