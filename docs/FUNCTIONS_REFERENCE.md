# ExchangeDB Functions Reference

ExchangeDB includes 1,198+ scalar functions and 120+ aggregate functions.
This document organizes them by category with descriptions and examples.

---

## Table of Contents

1. [String Functions](#string-functions)
2. [Math Functions](#math-functions)
3. [Date/Time Functions](#datetime-functions)
4. [Aggregate Functions](#aggregate-functions)
5. [Window Functions](#window-functions)
6. [Financial Functions](#financial-functions)
7. [Type Conversion Functions](#type-conversion-functions)
8. [Conditional Functions](#conditional-functions)
9. [System and Catalog Functions](#system-and-catalog-functions)
10. [Random Data Generator Functions](#random-data-generator-functions)
11. [Array Functions](#array-functions)
12. [Geospatial Functions](#geospatial-functions)
13. [Cryptographic Functions](#cryptographic-functions)
14. [Exchange-Domain Functions](#exchange-domain-functions)

---

## String Functions

### Core String Functions

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `length(s)` | Returns length in characters | `length('hello')` | `5` |
| `char_length(s)` | Alias for `length` | `char_length('hello')` | `5` |
| `octet_length(s)` | Returns length in bytes | `octet_length('hello')` | `5` |
| `bit_length(s)` | Returns length in bits | `bit_length('hello')` | `40` |
| `upper(s)` | Converts to uppercase | `upper('hello')` | `'HELLO'` |
| `lower(s)` | Converts to lowercase | `lower('HELLO')` | `'hello'` |
| `initcap(s)` | Capitalizes first letter of each word | `initcap('hello world')` | `'Hello World'` |

### Trimming and Padding

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `trim(s)` | Removes leading and trailing whitespace | `trim('  hi  ')` | `'hi'` |
| `ltrim(s)` | Removes leading whitespace | `ltrim('  hi')` | `'hi'` |
| `rtrim(s)` | Removes trailing whitespace | `rtrim('hi  ')` | `'hi'` |
| `trim(chars FROM s)` | Removes specific characters | `trim('x' FROM 'xxhixx')` | `'hi'` |
| `lpad(s, len, fill)` | Left-pads to length | `lpad('42', 5, '0')` | `'00042'` |
| `rpad(s, len, fill)` | Right-pads to length | `rpad('hi', 5, '.')` | `'hi...'` |

### Substring and Position

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `substr(s, start, len)` | Extracts substring | `substr('hello', 2, 3)` | `'ell'` |
| `substring(s, start, len)` | Alias for `substr` | `substring('hello', 1, 3)` | `'hel'` |
| `left(s, n)` | Returns first n characters | `left('hello', 3)` | `'hel'` |
| `right(s, n)` | Returns last n characters | `right('hello', 3)` | `'llo'` |
| `position(sub IN s)` | Returns position of substring | `position('lo' IN 'hello')` | `4` |
| `strpos(s, sub)` | Alias for position | `strpos('hello', 'lo')` | `4` |

### Concatenation and Replacement

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `concat(a, b, ...)` | Concatenates strings | `concat('a', 'b', 'c')` | `'abc'` |
| `concat_ws(sep, ...)` | Concatenate with separator | `concat_ws(',', 'a', 'b')` | `'a,b'` |
| `replace(s, from, to)` | Replaces all occurrences | `replace('hello', 'l', 'r')` | `'herro'` |
| `overlay(s, r, start, len)` | Replaces substring | `overlay('hello', 'XX', 2, 3)` | `'hXXo'` |
| `repeat(s, n)` | Repeats string n times | `repeat('ab', 3)` | `'ababab'` |
| `reverse(s)` | Reverses the string | `reverse('hello')` | `'olleh'` |

### Pattern Matching

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `starts_with(s, prefix)` | Checks if string starts with prefix | `starts_with('hello', 'hel')` | `true` |
| `ends_with(s, suffix)` | Checks if string ends with suffix | `ends_with('hello', 'llo')` | `true` |
| `regexp_match(s, pattern)` | Regex match | `regexp_match('abc123', '\d+')` | `'123'` |
| `regexp_replace(s, p, r)` | Regex replace | `regexp_replace('abc123', '\d+', 'N')` | `'abcN'` |
| `like(s, pattern)` | SQL LIKE pattern match | `like('hello', 'hel%')` | `true` |

### Splitting and Joining

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `split_part(s, delim, n)` | Returns nth part after splitting | `split_part('a.b.c', '.', 2)` | `'b'` |
| `translate(s, from, to)` | Character-by-character translation | `translate('hello', 'el', 'ip')` | `'hippo'` |
| `ascii(s)` | Returns ASCII code of first char | `ascii('A')` | `65` |
| `chr(n)` | Returns character from ASCII code | `chr(65)` | `'A'` |
| `encode(s, format)` | Encodes as base64 | `encode('hello', 'base64')` | `'aGVsbG8='` |
| `decode(s, format)` | Decodes from base64 | `decode('aGVsbG8=', 'base64')` | `'hello'` |

---

## Math Functions

### Basic Math

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `abs(x)` | Absolute value | `abs(-5.0)` | `5.0` |
| `ceil(x)` / `ceiling(x)` | Round up | `ceil(4.3)` | `5` |
| `floor(x)` | Round down | `floor(4.7)` | `4` |
| `round(x)` | Round to nearest integer | `round(4.5)` | `5` |
| `round(x, d)` | Round to d decimal places | `round(3.14159, 2)` | `3.14` |
| `trunc(x)` | Truncate toward zero | `trunc(4.7)` | `4` |
| `sign(x)` | Sign (-1, 0, or 1) | `sign(-5)` | `-1` |
| `mod(x, y)` | Modulus | `mod(10, 3)` | `1` |

### Powers and Roots

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `sqrt(x)` | Square root | `sqrt(16.0)` | `4.0` |
| `cbrt(x)` | Cube root | `cbrt(27.0)` | `3.0` |
| `pow(x, y)` / `power(x, y)` | x raised to y | `pow(2, 10)` | `1024` |
| `exp(x)` | e raised to x | `exp(1)` | `2.71828...` |

### Logarithms

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `ln(x)` | Natural logarithm | `ln(2.71828)` | `~1.0` |
| `log(x)` | Base-10 logarithm | `log(100)` | `2.0` |
| `log2(x)` | Base-2 logarithm | `log2(1024)` | `10.0` |
| `log10(x)` | Base-10 logarithm | `log10(1000)` | `3.0` |

### Trigonometric Functions

| Function | Description |
|----------|-------------|
| `sin(x)` | Sine (radians) |
| `cos(x)` | Cosine (radians) |
| `tan(x)` | Tangent (radians) |
| `asin(x)` | Inverse sine |
| `acos(x)` | Inverse cosine |
| `atan(x)` | Inverse tangent |
| `atan2(y, x)` | Two-argument inverse tangent |
| `degrees(x)` | Radians to degrees |
| `radians(x)` | Degrees to radians |
| `pi()` | The constant pi |

### Selection Functions

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `greatest(a, b, ...)` | Returns the largest value | `greatest(1, 5, 3)` | `5` |
| `least(a, b, ...)` | Returns the smallest value | `least(1, 5, 3)` | `1` |
| `clamp(x, lo, hi)` | Clamp value to range | `clamp(15, 0, 10)` | `10` |

---

## Date/Time Functions

### Current Time

| Function | Description | Example |
|----------|-------------|---------|
| `now()` | Current timestamp (nanos) | `SELECT now()` |
| `current_timestamp` | Same as `now()` | `SELECT current_timestamp` |
| `systimestamp()` | System clock timestamp | `SELECT systimestamp()` |
| `current_date` | Current date | `SELECT current_date` |

### Date Arithmetic

| Function | Description | Example |
|----------|-------------|---------|
| `dateadd(unit, n, ts)` | Add interval to timestamp | `dateadd('d', 7, now())` |
| `datediff(unit, ts1, ts2)` | Difference between timestamps | `datediff('h', ts1, ts2)` |
| `date_trunc(unit, ts)` | Truncate to unit boundary | `date_trunc('hour', now())` |

**Units for dateadd/datediff/date_trunc:**

| Unit | Aliases |
|------|---------|
| `'y'` | `'year'`, `'yyyy'` |
| `'M'` | `'month'` |
| `'w'` | `'week'` |
| `'d'` | `'day'`, `'dd'` |
| `'h'` | `'hour'`, `'hh'` |
| `'m'` | `'minute'`, `'mi'` |
| `'s'` | `'second'`, `'ss'` |
| `'T'` | `'millisecond'`, `'ms'` |
| `'u'` | `'microsecond'`, `'us'` |

### Extraction

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `extract(YEAR FROM ts)` | Extract component | `extract(YEAR FROM '2024-03-01'::TIMESTAMP)` | `2024` |
| `year(ts)` | Extract year | `year(now())` | `2026` |
| `month(ts)` | Extract month (1-12) | `month(now())` | `3` |
| `day(ts)` | Extract day of month | `day(now())` | `21` |
| `hour(ts)` | Extract hour (0-23) | `hour(now())` | current hour |
| `minute(ts)` | Extract minute (0-59) | `minute(now())` | current minute |
| `second(ts)` | Extract second (0-59) | `second(now())` | current second |
| `day_of_week(ts)` | Day of week (1=Mon, 7=Sun) | `day_of_week(now())` | `5` (Friday) |
| `day_of_year(ts)` | Day of year (1-366) | `day_of_year(now())` | `80` |
| `epoch(ts)` | Unix timestamp in seconds | `epoch(now())` | epoch seconds |

### Formatting

| Function | Description | Example |
|----------|-------------|---------|
| `to_str(ts, format)` | Format timestamp as string | `to_str(now(), 'yyyy-MM-dd HH:mm:ss')` |
| `to_timestamp(s, format)` | Parse string to timestamp | `to_timestamp('2024-03-01', 'yyyy-MM-dd')` |

**Format patterns:**

| Pattern | Description | Example |
|---------|-------------|---------|
| `yyyy` | 4-digit year | `2024` |
| `yy` | 2-digit year | `24` |
| `MM` | Month (01-12) | `03` |
| `dd` | Day (01-31) | `15` |
| `HH` | Hour 24h (00-23) | `14` |
| `mm` | Minute (00-59) | `30` |
| `ss` | Second (00-59) | `45` |
| `SSS` | Millisecond | `123` |
| `SSSSSS` | Microsecond | `123456` |
| `SSSSSSSSS` | Nanosecond | `123456789` |

---

## Aggregate Functions

### Standard Aggregates

| Function | Description | Example |
|----------|-------------|---------|
| `count(*)` | Count all rows | `SELECT count(*) FROM trades` |
| `count(x)` | Count non-NULL values | `SELECT count(price) FROM trades` |
| `count_distinct(x)` | Count distinct values | `SELECT count_distinct(symbol) FROM trades` |
| `sum(x)` | Sum of values | `SELECT sum(volume) FROM trades` |
| `avg(x)` | Arithmetic mean | `SELECT avg(price) FROM trades` |
| `min(x)` | Minimum value | `SELECT min(price) FROM trades` |
| `max(x)` | Maximum value | `SELECT max(price) FROM trades` |
| `first(x)` | First value (by order) | `SELECT first(price) FROM trades` |
| `last(x)` | Last value (by order) | `SELECT last(price) FROM trades` |

### Statistical Aggregates

| Function | Description | Example |
|----------|-------------|---------|
| `stddev(x)` | Sample standard deviation | `SELECT stddev(price) FROM trades` |
| `stddev_pop(x)` | Population standard deviation | `SELECT stddev_pop(price) FROM trades` |
| `stddev_samp(x)` | Sample standard deviation | Same as `stddev(x)` |
| `variance(x)` | Sample variance | `SELECT variance(price) FROM trades` |
| `var_pop(x)` | Population variance | `SELECT var_pop(price) FROM trades` |
| `var_samp(x)` | Sample variance | Same as `variance(x)` |
| `median(x)` | Median value | `SELECT median(price) FROM trades` |
| `mode(x)` | Most frequent value | `SELECT mode(symbol) FROM trades` |

### Percentile Aggregates

| Function | Description | Example |
|----------|-------------|---------|
| `percentile_cont(p)` | Continuous percentile | `SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY price) FROM trades` |
| `percentile_disc(p)` | Discrete percentile | `SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY price) FROM trades` |

### Regression Aggregates

| Function | Description |
|----------|-------------|
| `corr(y, x)` | Correlation coefficient |
| `covar_pop(y, x)` | Population covariance |
| `covar_samp(y, x)` | Sample covariance |
| `regr_slope(y, x)` | Slope of linear regression |
| `regr_intercept(y, x)` | Intercept of linear regression |

### Boolean Aggregates

| Function | Description |
|----------|-------------|
| `bool_and(x)` | True if all values are true |
| `bool_or(x)` | True if any value is true |

### Collection Aggregates

| Function | Description | Example |
|----------|-------------|---------|
| `string_agg(x, sep)` | Concatenate with separator | `SELECT string_agg(symbol, ',') FROM trades` |
| `array_agg(x)` | Collect values into array | `SELECT array_agg(price) FROM trades` |

### Compensated Summation

| Function | Description |
|----------|-------------|
| `ksum(x)` | Kahan compensated sum (more accurate than `sum` for floats) |
| `nsum(x)` | Neumaier compensated sum (even more accurate) |

---

## Window Functions

All window functions use the `OVER (...)` clause:

### Ranking

| Function | Description |
|----------|-------------|
| `row_number()` | Sequential row number within partition |
| `rank()` | Rank with gaps for ties |
| `dense_rank()` | Rank without gaps |
| `ntile(n)` | Distribute rows into n buckets |

### Distribution

| Function | Description |
|----------|-------------|
| `percent_rank()` | Relative rank (0 to 1) |
| `cume_dist()` | Cumulative distribution (0 to 1) |

### Offset

| Function | Description |
|----------|-------------|
| `lag(x, n, default)` | Value n rows before current |
| `lead(x, n, default)` | Value n rows after current |
| `first_value(x)` | First value in window frame |
| `last_value(x)` | Last value in window frame |
| `nth_value(x, n)` | Nth value in window frame |

### Running Aggregates

Any standard aggregate can be used as a window function:

```sql
SELECT symbol, timestamp, price,
    SUM(volume) OVER (PARTITION BY symbol ORDER BY timestamp) AS cumulative_vol,
    AVG(price) OVER (PARTITION BY symbol ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20
FROM trades;
```

---

## Financial Functions

### Aggregate Financial Functions

| Function | Description | Example |
|----------|-------------|---------|
| `vwap(price, volume)` | Volume-Weighted Average Price | `SELECT vwap(price, volume) FROM trades` |
| `ema(x, period)` | Exponential Moving Average | `SELECT ema(price, 20) FROM trades` |
| `sma(x, period)` | Simple Moving Average | `SELECT sma(price, 50) FROM trades` |
| `wma(x, period)` | Weighted Moving Average | `SELECT wma(price, 20) FROM trades` |
| `rsi(x, period)` | Relative Strength Index (0-100) | `SELECT rsi(price, 14) FROM trades` |
| `macd_signal(x, fast, slow, signal)` | MACD Signal line | `SELECT macd_signal(price, 12, 26, 9) FROM trades` |
| `bollinger_upper(x, period, stddev)` | Upper Bollinger Band | `SELECT bollinger_upper(price, 20, 2.0) FROM trades` |
| `bollinger_lower(x, period, stddev)` | Lower Bollinger Band | `SELECT bollinger_lower(price, 20, 2.0) FROM trades` |
| `atr(high, low, close, period)` | Average True Range | `SELECT atr(high, low, close, 14) FROM ohlcv` |
| `drawdown(x)` | Maximum Drawdown | `SELECT drawdown(price) FROM trades` |

**Example: Full Technical Analysis**

```sql
SELECT symbol,
    vwap(price, volume)               AS vwap,
    ema(price, 12)                    AS ema_12,
    ema(price, 26)                    AS ema_26,
    sma(price, 50)                    AS sma_50,
    rsi(price, 14)                    AS rsi_14,
    bollinger_upper(price, 20, 2.0)   AS bb_upper,
    bollinger_lower(price, 20, 2.0)   AS bb_lower,
    drawdown(price)                   AS max_drawdown
FROM trades
GROUP BY symbol;
```

### Exchange-Domain Scalar Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ohlcv_vwap(o, h, l, c, v)` | VWAP from OHLCV bar | `SELECT ohlcv_vwap(open, high, low, close, volume)` |
| `mid_price(bid, ask)` | Mid price from bid/ask | `SELECT mid_price(bid, ask) FROM quotes` |
| `spread(bid, ask)` | Absolute spread | `SELECT spread(bid, ask) FROM quotes` |
| `spread_bps(bid, ask)` | Spread in basis points | `SELECT spread_bps(bid, ask) FROM quotes` |
| `tick_delta(price, prev)` | Price change in ticks | `SELECT tick_delta(price, lag(price, 1) OVER (...))` |

---

## Type Conversion Functions

| Function | Description | Example |
|----------|-------------|---------|
| `cast(x AS type)` | Convert to type | `CAST(price AS INT)` |
| `to_int(x)` | Convert to integer | `to_int('42')` |
| `to_long(x)` | Convert to long | `to_long('123456789')` |
| `to_double(x)` | Convert to double | `to_double('3.14')` |
| `to_float(x)` | Convert to float | `to_float('3.14')` |
| `to_str(x, fmt)` | Convert to string | `to_str(now(), 'yyyy-MM-dd')` |
| `to_timestamp(s, fmt)` | Parse to timestamp | `to_timestamp('2024-03-01', 'yyyy-MM-dd')` |
| `to_date(s, fmt)` | Parse to date | `to_date('2024-03-01', 'yyyy-MM-dd')` |
| `to_boolean(x)` | Convert to boolean | `to_boolean('true')` |
| `to_char(x)` | Convert to character | `to_char(65)` |
| `pg_typeof(x)` | Returns the type name | `pg_typeof(42)` -> `'I32'` |

---

## Conditional Functions

| Function | Description | Example |
|----------|-------------|---------|
| `coalesce(a, b, ...)` | First non-NULL value | `coalesce(fee, 0.0)` |
| `nullif(a, b)` | NULL if a = b | `nullif(volume, 0)` |
| `greatest(a, b, ...)` | Largest value | `greatest(bid, ask)` |
| `least(a, b, ...)` | Smallest value | `least(bid, ask)` |
| `if(cond, then, else)` | Conditional value | `if(price > 50000, 'high', 'low')` |
| `ifnull(x, default)` | Default if NULL | `ifnull(fee, 0.0)` |
| `decode(x, v1, r1, ...)` | Multi-way conditional | `decode(side, 'buy', 1, 'sell', -1, 0)` |

---

## System and Catalog Functions

| Function | Description | Example |
|----------|-------------|---------|
| `version()` | Server version | `SELECT version()` |
| `current_database()` | Current database name | `SELECT current_database()` |
| `current_user` | Current user name | `SELECT current_user` |
| `current_schema` | Current schema name | `SELECT current_schema` |
| `pg_catalog.*` | PostgreSQL catalog access | Used internally by PG clients |
| `information_schema.*` | Standard schema metadata | Used internally by PG clients |

---

## Random Data Generator Functions

Useful for testing and generating synthetic data:

| Function | Description | Example |
|----------|-------------|---------|
| `rnd_int(lo, hi, null_pct)` | Random integer | `rnd_int(0, 100, 0)` |
| `rnd_long(lo, hi, null_pct)` | Random long | `rnd_long(0, 1000000, 0)` |
| `rnd_double(null_pct)` | Random double (0.0-1.0) | `rnd_double(0)` |
| `rnd_float(null_pct)` | Random float (0.0-1.0) | `rnd_float(0)` |
| `rnd_str(lo, hi, null_pct)` | Random string | `rnd_str(5, 10, 0)` |
| `rnd_symbol(values...)` | Random from list | `rnd_symbol('BTC', 'ETH', 'SOL')` |
| `rnd_timestamp(lo, hi, null_pct)` | Random timestamp | `rnd_timestamp(...)` |
| `rnd_boolean()` | Random boolean | `rnd_boolean()` |
| `rnd_uuid4()` | Random UUID | `rnd_uuid4()` |
| `rnd_byte(lo, hi)` | Random byte | `rnd_byte(0, 127)` |
| `rnd_short(lo, hi)` | Random short | `rnd_short(0, 1000)` |

**Example: Generate 1 million rows of synthetic trade data:**

```sql
SELECT
    rnd_timestamp(
        to_timestamp('2024-01-01', 'yyyy-MM-dd'),
        to_timestamp('2024-12-31', 'yyyy-MM-dd'),
        0
    ) AS timestamp,
    rnd_symbol('BTC/USD', 'ETH/USD', 'SOL/USD', 'DOGE/USD') AS symbol,
    rnd_double(0) * 70000 AS price,
    rnd_double(0) * 100 AS volume,
    rnd_symbol('buy', 'sell') AS side
FROM long_sequence(1000000);
```

---

## Array Functions

| Function | Description | Example |
|----------|-------------|---------|
| `array_length(arr)` | Length of array | `array_length(tags)` |
| `array_contains(arr, val)` | Check if array contains value | `array_contains(tags, 'urgent')` |
| `array_agg(x)` | Aggregate values into array | `SELECT array_agg(symbol) FROM trades` |

---

## Geospatial Functions

ExchangeDB supports GeoHash column types. Geospatial query functions
(within_distance, make_geohash, etc.) are planned but not yet implemented.

| Function | Description | Status |
|----------|-------------|--------|
| `make_geohash(lat, lon, bits)` | Create geohash from coordinates | Planned |
| `within_distance(geo1, geo2, dist)` | Distance check | Planned |
| `geohash_to_str(geo)` | Convert geohash to string | Planned |

---

## Cryptographic Functions

| Function | Description | Example |
|----------|-------------|---------|
| `md5(s)` | MD5 hash | `md5('hello')` |
| `sha256(s)` | SHA-256 hash | `sha256('hello')` |

---

## Function Count by Category

| Category | Approximate Count |
|----------|-------------------|
| String functions | ~80 |
| Math functions | ~60 |
| Date/Time functions | ~50 |
| Standard aggregates | ~30 |
| Statistical aggregates | ~20 |
| Financial aggregates | ~15 |
| Window functions | ~12 |
| Type conversion | ~25 |
| Conditional functions | ~10 |
| Random generators | ~15 |
| System/catalog functions | ~20 |
| PostgreSQL compatibility | ~800+ (pg_catalog, information_schema stubs) |
| Exchange-domain functions | ~10 |
| Cryptographic functions | ~5 |
| Array functions | ~5 |
| Geospatial functions | ~5 (planned) |
| **Total** | **1,198+** |

The high total count includes per-type function variants (e.g., `sum` has
separate implementations for I32, I64, F32, F64, etc.) and PostgreSQL
compatibility shims used by client libraries and tools like DBeaver, Grafana,
and psql.
