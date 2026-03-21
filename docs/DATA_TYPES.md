# ExchangeDB Data Types Reference

ExchangeDB supports 41 column data types organized into fixed-width numeric
types, temporal types, string types, geospatial types, decimal types, container
types, and internal types.

---

## Table of Contents

1. [Type Summary Table](#type-summary-table)
2. [Boolean Type](#boolean-type)
3. [Integer Types](#integer-types)
4. [Floating-Point Types](#floating-point-types)
5. [Temporal Types](#temporal-types)
6. [String Types](#string-types)
7. [Symbol Type](#symbol-type)
8. [Binary Type](#binary-type)
9. [UUID Type](#uuid-type)
10. [IPv4 Type](#ipv4-type)
11. [Large Integer Types](#large-integer-types)
12. [Decimal Types](#decimal-types)
13. [Geospatial Types](#geospatial-types)
14. [Container Types](#container-types)
15. [PostgreSQL Compatibility Types](#postgresql-compatibility-types)
16. [Internal Types](#internal-types)
17. [SQL Name Mappings](#sql-name-mappings)
18. [NULL Handling](#null-handling)
19. [Type Casting](#type-casting)

---

## Type Summary Table

| # | Type | SQL Name(s) | Byte Size | NULL Sentinel | Category |
|---|------|-------------|-----------|---------------|----------|
| 0 | Boolean | `BOOLEAN`, `BOOL` | 1 | `0` (false) | Fixed |
| 1 | I8 | `BYTE`, `TINYINT` | 1 | `0` (min) | Fixed |
| 2 | I16 | `SHORT`, `SMALLINT` | 2 | `-32768` | Fixed |
| 3 | I32 | `INT`, `INTEGER` | 4 | `-2147483648` | Fixed |
| 4 | I64 | `LONG`, `BIGINT` | 8 | `-9223372036854775808` | Fixed |
| 5 | F32 | `FLOAT`, `REAL` | 4 | `NaN` | Fixed |
| 6 | F64 | `DOUBLE`, `DOUBLE PRECISION` | 8 | `NaN` | Fixed |
| 7 | Timestamp | `TIMESTAMP` | 8 | `i64::MIN` | Fixed |
| 8 | Symbol | `SYMBOL` | 4 | `-1` (invalid ID) | Fixed + Index |
| 9 | Varchar | `VARCHAR`, `TEXT` | Variable | Empty / null marker | Variable |
| 10 | Binary | `BINARY`, `BYTEA` | Variable | Empty / null marker | Variable |
| 11 | Uuid | `UUID` | 16 | All zeros | Fixed |
| 12 | Date | `DATE` | 4 | `i32::MIN` | Fixed |
| 13 | Char | `CHAR`, `CHARACTER` | 2 | `0` | Fixed |
| 14 | IPv4 | `IPv4` | 4 | `0` | Fixed |
| 15 | Long128 | `LONG128` | 16 | `i128::MIN` | Fixed |
| 16 | Long256 | `LONG256` | 32 | All zeros | Fixed |
| 17 | GeoHash | `GEOHASH` | 8 | `i64::MIN` | Fixed |
| 18 | String | `STRING` | Variable | Empty / null marker | Variable |
| 19 | TimestampMicro | `TIMESTAMP(6)` | 8 | `i64::MIN` | Fixed |
| 20 | TimestampMilli | `TIMESTAMP(3)` | 8 | `i64::MIN` | Fixed |
| 21 | Interval | `INTERVAL` | 16 | All zeros | Fixed |
| 22 | Decimal8 | `DECIMAL(2,1)` | 1 | `i8::MIN` | Fixed |
| 23 | Decimal16 | `DECIMAL(4,2)` | 2 | `i16::MIN` | Fixed |
| 24 | Decimal32 | `DECIMAL(9,4)` | 4 | `i32::MIN` | Fixed |
| 25 | Decimal64 | `DECIMAL(18,8)` | 8 | `i64::MIN` | Fixed |
| 26 | Decimal128 | `DECIMAL(38,16)` | 16 | `i128::MIN` | Fixed |
| 27 | Decimal256 | `DECIMAL(76,32)` | 32 | All zeros | Fixed |
| 28 | GeoByte | `GEOHASH(1c)` | 1 | `0` | Fixed |
| 29 | GeoShort | `GEOHASH(3c)` | 2 | `0` | Fixed |
| 30 | GeoInt | `GEOHASH(6c)` | 4 | `0` | Fixed |
| 31 | Array | `ARRAY` | Variable | Empty | Variable |
| 32 | Cursor | (internal) | 8 | N/A | Internal |
| 33 | Record | (internal) | 8 | N/A | Internal |
| 34 | RegClass | `REGCLASS` | 4 | `0` | PG Compat |
| 35 | RegProcedure | `REGPROCEDURE` | 4 | `0` | PG Compat |
| 36 | ArrayString | `TEXT[]` | Variable | Empty | PG Compat |
| 37 | Null | (internal) | 0 | N/A | Internal |
| 38 | VarArg | (internal) | 0 | N/A | Internal |
| 39 | Parameter | (internal) | 0 | N/A | Internal |
| 40 | VarcharSlice | (internal) | Variable | N/A | Internal |

---

## Boolean Type

```sql
CREATE TABLE flags (
    timestamp TIMESTAMP,
    is_active BOOLEAN
) TIMESTAMP(timestamp);

INSERT INTO flags VALUES (now(), true);
INSERT INTO flags VALUES (now(), false);
```

- **Storage**: 1 byte per value.
- **Values**: `true` / `false`, also accepts `TRUE`, `FALSE`, `t`, `f`, `1`, `0`.
- **NULL sentinel**: Represented as `0` with a separate null bitmap.

---

## Integer Types

### I8 (BYTE / TINYINT)

```sql
ALTER TABLE t ADD COLUMN flags BYTE;
```

- **Storage**: 1 byte, signed.
- **Range**: -128 to 127 (NULL sentinel uses `0`).

### I16 (SHORT / SMALLINT)

```sql
CREATE TABLE t (id SHORT);
```

- **Storage**: 2 bytes, signed.
- **Range**: -32,768 to 32,767.
- **NULL sentinel**: `-32768` (`i16::MIN`).

### I32 (INT / INTEGER)

```sql
CREATE TABLE t (count INT);
```

- **Storage**: 4 bytes, signed.
- **Range**: -2,147,483,648 to 2,147,483,647.
- **NULL sentinel**: `-2147483648` (`i32::MIN`).

### I64 (LONG / BIGINT)

```sql
CREATE TABLE t (big_count LONG);
```

- **Storage**: 8 bytes, signed.
- **Range**: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
- **NULL sentinel**: `i64::MIN`.

---

## Floating-Point Types

### F32 (FLOAT / REAL)

```sql
CREATE TABLE t (ratio FLOAT);
```

- **Storage**: 4 bytes, IEEE 754 single-precision.
- **Range**: approximately +/-3.4 x 10^38.
- **NULL sentinel**: `NaN`.
- **Note**: Use `DOUBLE` for financial data to avoid precision loss.

### F64 (DOUBLE / DOUBLE PRECISION)

```sql
CREATE TABLE trades (price DOUBLE, volume DOUBLE);
```

- **Storage**: 8 bytes, IEEE 754 double-precision.
- **Range**: approximately +/-1.8 x 10^308.
- **NULL sentinel**: `NaN`.
- **Recommended** for financial price and volume data.

---

## Temporal Types

### Timestamp (TIMESTAMP)

```sql
CREATE TABLE trades (
    timestamp TIMESTAMP,
    price DOUBLE
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

- **Storage**: 8 bytes, nanosecond precision.
- **Internal representation**: `i64` -- nanoseconds since Unix epoch (1970-01-01T00:00:00Z).
- **Range**: 1677-09-21 to 2262-04-11 (i64 nanosecond range).
- **NULL sentinel**: `i64::MIN`.
- **Literals**: ISO 8601 strings are automatically parsed:
  - `'2024-03-01'`
  - `'2024-03-01T10:30:00Z'`
  - `'2024-03-01T10:30:00.123456789Z'`

### TimestampMicro (TIMESTAMP(6))

- **Storage**: 8 bytes, microsecond precision.
- **Internal representation**: `i64` microseconds since epoch.

### TimestampMilli (TIMESTAMP(3))

- **Storage**: 8 bytes, millisecond precision.
- **Internal representation**: `i64` milliseconds since epoch.

### Date (DATE)

```sql
CREATE TABLE events (event_date DATE, description VARCHAR);
```

- **Storage**: 4 bytes.
- **Internal representation**: `i32` -- days since Unix epoch.
- **Range**: approximately 5.8 million years before and after epoch.
- **NULL sentinel**: `i32::MIN`.

### Interval (INTERVAL)

```sql
SELECT timestamp + INTERVAL '1 hour' FROM trades;
```

- **Storage**: 16 bytes (two `i64` values: start offset and duration).
- **Used in**: date arithmetic, `dateadd()`, `datediff()`.

---

## String Types

### Varchar (VARCHAR / TEXT)

```sql
CREATE TABLE logs (
    timestamp TIMESTAMP,
    message VARCHAR
) TIMESTAMP(timestamp);
```

- **Storage**: Variable length. Stored as a data file (`.d`) plus an offset
  index file (`.i`).
- **Encoding**: UTF-8.
- **Max length**: Limited only by available memory and disk.
- **NULL**: Tracked via null bitmap.

### String (STRING)

- Functionally identical to `VARCHAR`.
- Provided for compatibility with systems that distinguish between
  `VARCHAR` and `STRING`.

### Char (CHAR / CHARACTER)

```sql
CREATE TABLE t (grade CHAR);
```

- **Storage**: 2 bytes (single UTF-16 character).
- **Range**: Any single Unicode character.
- **NULL sentinel**: `0`.

---

## Symbol Type

```sql
CREATE TABLE trades (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    exchange SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

- **Storage**: 4 bytes per value (int32 index into a dictionary).
- **Dictionary**: Unique string values are stored once in a symbol map
  (`.k` and `.v` files). Each row stores only the integer ID.
- **Auto-indexed**: Symbol columns automatically get a bitmap index for
  fast equality lookups.
- **Ideal for**: Low-cardinality string columns (ticker symbols, exchange
  names, side indicators, currency codes).
- **Performance**: Equality filters on SYMBOL columns use the bitmap index
  and are significantly faster than VARCHAR filters.
- **NULL sentinel**: `-1` (invalid symbol ID).
- **ILP behavior**: ILP tags are stored as SYMBOL columns automatically.

---

## Binary Type

```sql
CREATE TABLE blobs (
    id LONG,
    payload BINARY
);
```

- **Storage**: Variable length.
- **Encoding**: Raw bytes.
- **Use case**: Storing serialized protobuf messages, binary order book
  snapshots, or any opaque data.

---

## UUID Type

```sql
CREATE TABLE orders (
    order_id UUID,
    timestamp TIMESTAMP,
    symbol SYMBOL
) TIMESTAMP(timestamp);

INSERT INTO orders VALUES (rnd_uuid4(), now(), 'BTC/USD');
```

- **Storage**: 16 bytes (128-bit UUID).
- **Format**: Standard UUID string format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
- **NULL sentinel**: All zero bytes.
- **Generation**: Use `rnd_uuid4()` to generate random UUIDs.

---

## IPv4 Type

```sql
CREATE TABLE connections (
    timestamp TIMESTAMP,
    client_ip IPv4
) TIMESTAMP(timestamp);
```

- **Storage**: 4 bytes (unsigned 32-bit integer).
- **Format**: Dotted-decimal notation (`192.168.1.1`).
- **NULL sentinel**: `0`.

---

## Large Integer Types

### Long128

```sql
CREATE TABLE t (big_value LONG128);
```

- **Storage**: 16 bytes (128-bit signed integer).
- **Range**: -2^127 to 2^127 - 1.
- **NULL sentinel**: `i128::MIN`.

### Long256

```sql
CREATE TABLE t (hash LONG256);
```

- **Storage**: 32 bytes (256-bit value stored as four `u64` words).
- **Use case**: Cryptographic hashes, blockchain addresses.
- **NULL sentinel**: All zero bytes.

---

## Decimal Types

Fixed-point decimal types for exact arithmetic without floating-point
rounding errors.

| Type | Size | Max Precision | Max Scale | Example |
|------|------|--------------|-----------|---------|
| Decimal8 | 1 byte | 2 | 1 | `9.9` |
| Decimal16 | 2 bytes | 4 | 2 | `99.99` |
| Decimal32 | 4 bytes | 9 | 4 | `99999.9999` |
| Decimal64 | 8 bytes | 18 | 8 | `9999999999.99999999` |
| Decimal128 | 16 bytes | 38 | 16 | Full 128-bit decimal |
| Decimal256 | 32 bytes | 76 | 32 | Full 256-bit decimal |

```sql
CREATE TABLE prices (
    timestamp TIMESTAMP,
    price DECIMAL(18,8)
) TIMESTAMP(timestamp);
```

---

## Geospatial Types

### GeoHash

```sql
CREATE TABLE locations (
    timestamp TIMESTAMP,
    geohash GEOHASH
) TIMESTAMP(timestamp);
```

- **Storage**: 8 bytes (i64 encoding of a geohash).
- **Precision**: Up to 60 bits of geospatial precision.
- **NULL sentinel**: `i64::MIN`.

### Sized Geohash Variants

| Type | SQL Name | Size | Precision |
|------|----------|------|-----------|
| GeoByte | `GEOHASH(1c)` | 1 byte | ~5 chars, ~2500 km |
| GeoShort | `GEOHASH(3c)` | 2 bytes | ~7 chars, ~76 m |
| GeoInt | `GEOHASH(6c)` | 4 bytes | ~12 chars, ~0.6 m |

```sql
CREATE TABLE t (location GEOHASH(6c));
```

---

## Container Types

### Array

```sql
CREATE TABLE t (
    timestamp TIMESTAMP,
    tags ARRAY
) TIMESTAMP(timestamp);
```

- **Storage**: Variable length, length-prefixed binary.
- **Use case**: Storing lists of values.

### ArrayString (TEXT[])

```sql
CREATE TABLE t (labels TEXT[]);
```

- PostgreSQL-compatible text array type.
- **Storage**: Variable length.

---

## PostgreSQL Compatibility Types

These types exist for compatibility with PostgreSQL system catalogs and
client libraries:

| Type | SQL Name | Size | Purpose |
|------|----------|------|---------|
| RegClass | `REGCLASS` | 4 bytes | Table OID reference |
| RegProcedure | `REGPROCEDURE` | 4 bytes | Function OID reference |

These are used internally by the `pg_catalog` and `information_schema`
compatibility layer.

---

## Internal Types

These types are used by the query engine internally and cannot be used in
`CREATE TABLE` statements:

| Type | Purpose |
|------|---------|
| Cursor | Reference to a cursor in the execution plan |
| Record | Reference to a record in the execution plan |
| Null | Represents the type of a literal NULL |
| VarArg | Variable argument marker for function signatures |
| Parameter | Bind parameter placeholder in prepared statements |
| VarcharSlice | Transient in-memory slice into varchar data |

---

## SQL Name Mappings

ExchangeDB accepts multiple SQL names for each type for PostgreSQL and
QuestDB compatibility:

| ExchangeDB Type | Accepted SQL Names |
|------------------|--------------------|
| Boolean | `BOOLEAN`, `BOOL` |
| I8 | `BYTE`, `TINYINT`, `INT1` |
| I16 | `SHORT`, `SMALLINT`, `INT2` |
| I32 | `INT`, `INTEGER`, `INT4` |
| I64 | `LONG`, `BIGINT`, `INT8` |
| F32 | `FLOAT`, `REAL`, `FLOAT4` |
| F64 | `DOUBLE`, `DOUBLE PRECISION`, `FLOAT8` |
| Timestamp | `TIMESTAMP`, `TIMESTAMP WITHOUT TIME ZONE` |
| Symbol | `SYMBOL` |
| Varchar | `VARCHAR`, `TEXT`, `STRING`, `CHARACTER VARYING` |
| Binary | `BINARY`, `BYTEA`, `BLOB` |
| Uuid | `UUID` |
| Date | `DATE` |
| Char | `CHAR`, `CHARACTER` |
| IPv4 | `IPv4` |

---

## NULL Handling

ExchangeDB uses sentinel values for fixed-width types and a null bitmap
for variable-width types.

**Testing for NULL:**

```sql
SELECT * FROM trades WHERE price IS NULL;
SELECT * FROM trades WHERE price IS NOT NULL;
```

**COALESCE:**

```sql
SELECT coalesce(price, 0.0) AS price FROM trades;
```

**NULLIF:**

```sql
SELECT nullif(price, 0.0) AS price FROM trades;
```

**Aggregate behavior:** All aggregate functions (SUM, AVG, etc.) skip NULL
values by default, consistent with SQL standard behavior.

---

## Type Casting

### Explicit CAST

```sql
SELECT CAST(price AS INT) FROM trades;
SELECT CAST('2024-03-01' AS TIMESTAMP);
SELECT CAST(123 AS VARCHAR);
```

### Double-colon syntax (PostgreSQL style)

```sql
SELECT price::INT FROM trades;
SELECT '2024-03-01'::TIMESTAMP;
```

### Implicit casting

ExchangeDB performs implicit widening casts:

- `I8` -> `I16` -> `I32` -> `I64`
- `F32` -> `F64`
- `I32` -> `F64`
- `Date` -> `Timestamp`

Narrowing casts (e.g., `I64` to `I32`) require an explicit `CAST`.
