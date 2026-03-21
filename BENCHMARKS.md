# ExchangeDB Performance Benchmarks

Generated: 2026-03-21

## System Info
- OS: macOS (Darwin 25.2.0)
- CPU: Apple Silicon (M-series)
- Rust: 1.85 (workspace `rust-version`)
- Profile: `bench` (optimized, release)
- Framework: Criterion 0.5

## Codebase Stats
- **183,832+** lines of Rust across **332+** source files
- **19,598+** tests passing (3 pre-existing failures, 5 ignored) across 6 crates
- Wave 25 optimizations: vectorized GROUP BY filter pushdown (IN, LIKE, IS NULL, Expression), NOT BETWEEN support, constant expression evaluation fix, CASE WHEN in WHERE fix

---

## Storage Engine

| Operation | Throughput (median) | Latency (median) |
|-----------|-----------|-------------------|
| Column write f64 (1M) | 198.78 M/s | 5.03 ms |
| Column read f64 (1M) | 1.05 G/s | 949 us |
| Column read f64 slice (1M) | 759.75 M/s | 1.32 ms |
| Column write i64 (1M) | 250.76 M/s | 3.99 ms |
| Var column write (100K strings) | 36.93 M/s | 2.71 ms |
| Mmap append (1M x 8B) | 172.88 M/s | 5.78 ms |
| Ring buffer (1M push/pop) | 674.84 M/s | 1.48 ms |
| Symbol map lookup (1M, 1K symbols) | 52.63 M/s | 19.00 ms |
| Symbol map insert (100K) | 3.54 M/s | 28.23 ms |
| LZ4 compress (1M f64) | 18.22 GiB/s | 409 us |
| LZ4 decompress (1M f64) | 62.93 MiB/s | 476 us |
| LZ4 compress (1M timestamps) | 526.81 MiB/s | 14.48 ms |
| Delta encode (1M timestamps) | 5.59 G/s | 179 us |
| Delta decode (1M timestamps) | 1.13 G/s | 886 us |
| SIMD sum f64 (1M) | 4.49 G/s | 223 us |
| SIMD min f64 (1M) | 3.40 G/s | 294 us |
| SIMD max f64 (1M) | 3.40 G/s | 294 us |
| Scalar sum f64 (1M) | 1.13 G/s | 882 us |
| Scalar min f64 (1M) | 7.20 G/s | 139 us |
| Scalar max f64 (1M) | 7.15 G/s | 139 us |
| Bitmap index lookup (1 key) | 767.50 M/s | 45.5 us |
| Full scan for 1 key (1M) | 532.93 M/s | 1.88 ms |

## Table-Level I/O

| Operation | Throughput (median) | Latency (median) |
|-----------|-----------|-------------------|
| Table writer (100K rows) | 7.18 M rows/s | 13.92 ms |
| Table writer (500K rows) | 10.27 M rows/s | 48.70 ms |
| Table writer (1M rows) | 4.40 M rows/s | 227.28 ms |
| Table reader scan (1M, 2 cols) | 339.04 M/s | 2.95 ms |
| Batch write (100K rows) | 15.51 M rows/s | 6.45 ms |
| Batch write (500K rows) | 14.96 M rows/s | 33.42 ms |
| Batch write (1M rows) | 13.78 M rows/s | 72.59 ms |
| Batch write raw (100K rows) | 6.70 M rows/s | 14.92 ms |
| Batch write raw (500K rows) | 20.49 M rows/s | 24.40 ms |
| Batch write raw (1M rows) | 18.80 M rows/s | 53.20 ms |
| Write preallocated (1M, single part) | 8.16 M rows/s | 122.55 ms |
| Partition pruning full (30 parts, 1M) | 394.97 M/s | 2.53 ms |
| Partition pruning 1-of-30 | 733.27 M/s | 45.5 us |
| Concurrent R+W (100K) | 9.18 M/s | 10.90 ms |
| Symbol map scale (10K syms, 1M lookups) | 46.25 M/s | 20.27 ms |
| Symbol map scale (100K syms, 1M lookups) | 36.61 M/s | 27.32 ms |

### WAL Write Performance

| Operation | Throughput | Latency (median) |
|-----------|-----------|-------------------|
| WAL write + commit (100K rows) | 990 K rows/s | 100.97 ms |
| WAL write deferred merge (100K rows) | 8.09 M rows/s | 12.36 ms |

---

## Query Engine

| Query | Rows | Throughput (median) | Latency (median) |
|-------|------|-----------|-------------------|
| Full scan (`SELECT *`) | 1M | 6.15 M rows/s | 162.70 ms |
| Filtered scan (`price > 50000`) | 1M | 5.05 M rows/s | 198.14 ms |
| GROUP BY (100 symbols) | 1M | 11.64 M rows/s | 85.89 ms |
| SAMPLE BY 1h | 1M | 6.68 M rows/s | 149.77 ms |
| LATEST ON (100 symbols) | 1M | 4.07 M rows/s | 245.86 ms |
| ORDER BY + LIMIT 100 | 1M | 4.12 M rows/s | 242.76 ms |
| Aggregate (count/sum/avg/min/max) | 1M | 4.00 M rows/s | 250.21 ms |
| Multi-partition scan (30 parts) | 1M | 17.30 M rows/s | 57.81 ms |
| Insert throughput (100K rows, 5 cols) | 100K | 9.31 M rows/s | 10.74 ms |

### Parallel vs Sequential (30 partitions, 1M rows)

| Mode | Throughput | Latency |
|------|-----------|---------|
| Parallel | 13.08 M rows/s | 76.47 ms |
| Sequential | 6.77 M rows/s | 147.77 ms |

### SAMPLE BY Intervals (1M rows, 30 partitions)

| Interval | Throughput | Latency |
|----------|-----------|---------|
| 1 minute | 10.67 M rows/s | 93.76 ms |
| 1 hour | 12.23 M rows/s | 81.77 ms |
| 1 day | 12.28 M rows/s | 81.47 ms |

### Compiled vs Interpreted Filters (1M rows)

| Filter | Throughput | Latency |
|--------|-----------|---------|
| Compiled simple | 94.36 M/s | 10.60 ms |
| Interpreted simple | 143.71 M/s | 6.96 ms |
| Compiled complex | 55.23 M/s | 18.11 ms |
| Interpreted complex | 341.39 M/s | 2.93 ms |

### Top-K vs Sort+Limit (1M rows)

| K | Throughput | Latency |
|---|-----------|---------|
| Top 10 | 3.28 M rows/s | 304.57 ms |
| Top 100 | 3.86 M rows/s | 258.95 ms |
| Top 10,000 | 2.01 M rows/s | 498.62 ms |

### GROUP BY Strategies (1M rows)

| Groups | Throughput | Latency |
|--------|-----------|---------|
| 10 groups | 10.14 M rows/s | 86.45 ms |
| 100 groups | 9.93 M rows/s | 100.72 ms |
| 1000 groups | 8.21 M rows/s | 121.80 ms |

### Filter Pushdown (1M rows)

| Scenario | Throughput | Latency |
|----------|-----------|---------|
| With filter (`price > 51000`) | 8.09 M rows/s | 123.67 ms |
| Full scan (no filter) | 4.94 M rows/s | 202.26 ms |
| Highly selective (AND) | 5.88 M rows/s | 169.97 ms |

### ASOF JOIN

| Scenario | Throughput | Latency |
|----------|-----------|---------|
| 100K trades / 500K quotes / 100 symbols | 1.35 K rows/s | 74.15 s |

> **Note**: ASOF JOIN performance is an outlier -- the current naive O(n*m) implementation needs optimization with sorted merge or index-based lookup.

### Query Aggregation (SIMD, 1M rows)

| Aggregate | Throughput | Latency |
|-----------|-----------|---------|
| sum(price) | 14.94 M rows/s | 66.95 ms |
| Multi-agg (count/sum/avg/min/max) | 9.36 M rows/s | 106.82 ms |
| count(*) | 11.10 M rows/s | 90.09 ms |
| min/max | 12.70 M rows/s | 78.76 ms |

### Multi-Partition GROUP BY (30 partitions, 1M rows)

| Scenario | Throughput | Latency |
|----------|-----------|---------|
| 100 groups | 61.05 M rows/s | 16.38 ms |

### LATEST ON at Scale

| Scenario | Throughput | Latency |
|----------|-----------|---------|
| 100K rows / 100 symbols | 6.36 M rows/s | 15.72 ms |
| 1M rows / 1000 symbols | 6.19 M rows/s | 161.62 ms |

---

## Insert Performance

| Scenario | Throughput | Latency |
|----------|-----------|---------|
| Table writer (single partition, 1M) | 4.40 M rows/s | 227 ms |
| Table writer (30 partitions, 1M) | ~10.27 M rows/s | 49 ms (500K) |
| Batch write (1M, columnar) | 13.78 M rows/s | 73 ms |
| Batch write raw (1M, index-based) | 18.80 M rows/s | 53 ms |
| Write preallocated (1M, steady-state) | 8.16 M rows/s | 123 ms |
| With WAL (100K rows) | 990 K rows/s | 101 ms |
| WAL deferred merge (100K rows) | 8.09 M rows/s | 12.4 ms |
| SQL INSERT batches (100K rows) | 50.52 K rows/s | 1.98 s |

---

## TSBS DevOps (1M rows, 100 hosts)

| Query | Throughput (median) | Latency (median) |
|-------|-----------|-------------------|
| Last point per host | 7.15 M rows/s | 139.87 ms |
| Max CPU 12h | 1.19 G rows/s | 840 us |
| Double GROUP BY (SAMPLE BY 1h) | 9.50 M rows/s | 105.25 ms |
| High CPU (filtered, top 10) | 15.22 M rows/s | 65.71 ms |
| GROUP BY + ORDER BY + LIMIT 5 | 41.25 M rows/s | 24.24 ms |
| Aggregate all | 13.66 M rows/s | 73.23 ms |

### TSBS Insert (100K rows, 10 columns)

| Hosts | Throughput | Latency |
|-------|-----------|---------|
| 10 | 469 K rows/s | 213 ms |
| 100 | 514 K rows/s | 194 ms |
| 1,000 | 725 K rows/s | 138 ms |

---

## Exchange-Specific

| Operation | Throughput (median) | Latency (median) |
|----------|-----------|-------------------|
| OHLCV aggregation (1M ticks, S1) | 219.59 M/s | 4.55 ms |
| OrderBook delta apply (100K) | 89.89 M/s | 1.11 ms |
| Delta encode prices (1M) | 1.25 G/s | 798 us |
| Delta decode prices (1M) | 851.44 M/s | 1.17 ms |

---

## E2E SQL Pipeline (parse + plan + execute, 100K rows)

| Query | Throughput | Latency |
|-------|-----------|---------|
| SELECT * | 4.67 M rows/s | 21.41 ms |
| SELECT with filter | 7.09 M rows/s | 14.10 ms |
| SELECT aggregate | 8.14 M rows/s | 12.29 ms |
| CREATE TABLE | 1.52 K ops/s | 659 us |
| INSERT (100K via SQL batches) | 50.52 K rows/s | 1.98 s |

---

## Comparison vs QuestDB Published Numbers

QuestDB publishes TSBS benchmark results for their production-grade Java-based engine.
Below is an approximate comparison using publicly available QuestDB TSBS numbers
(from questdb.io/blog, single-node, AMD EPYC / similar server-class hardware):

| Metric | ExchangeDB (Apple M-series) | QuestDB (published) | Notes |
|--------|---------------------------|---------------------|-------|
| TSBS Insert (100 hosts) | 514 K rows/s | ~900 K rows/s | QuestDB on server hardware with ILP |
| Last point per host | 7.15 M rows/s | ~10-15 M rows/s | QuestDB uses JIT-compiled queries |
| Max CPU 12h (time-range filter) | 1.19 G rows/s | N/A (different metric) | ExchangeDB partition pruning very effective |
| GROUP BY + ORDER BY + LIMIT | 41.25 M rows/s | ~20-50 M rows/s | Comparable range |
| Column read throughput | 590.70 M elem/s | ~800 M elem/s (mmap) | Both use mmap, QuestDB has JIT |
| SIMD sum (f64, 1M) | 4.49 G elem/s | ~4-6 G elem/s | Both leverage SIMD intrinsics |

> **Disclaimer**: Direct comparison is approximate. QuestDB benchmarks run on server-grade
> hardware (higher memory bandwidth, more cores), have JIT-compiled filters, and use their
> proprietary ILP ingestion protocol. ExchangeDB numbers are on a laptop-class Apple Silicon chip.

---

## Query Latency (v0.1.1)

Measured with `profile-query` on real OHLCV data (504 rows, mmap cache warm).
These numbers reflect the full SQL pipeline: parse + plan + optimize + execute.

| Query | Execute (p50) | Notes |
|-------|--------------|-------|
| SELECT * LIMIT 1 | 4.0 µs | Single-row fetch |
| SELECT * LIMIT 25 | 11.6 µs | ~100-200x faster than v0.1.0 (~2 ms) |
| SELECT * LIMIT 100 | 20.4 µs | Sub-25 µs for 100-row scan |
| SELECT * (all 504 rows) | 58.6 µs | Full table scan |
| COUNT(*) | 21.4 µs | Aggregate pushdown |
| SAMPLE BY 4h | 88.8 µs | Time-bucketed aggregation |
| LATEST ON | 72.1 µs | Last value per symbol |

**Key improvements over v0.1.0:**
- Simple scans (LIMIT 25/100): **~2 ms -> ~11-20 µs** (100-200x speedup)
- Achieved via: mmap cache, table registry, optimizer skip for small tables, limit pushdown

---

## Key Highlights

- **Batch write (columnar)** achieves **18.80 M rows/s** for 1M rows -- 4.3x faster than row-at-a-time writer.
- **Column reads** sustain **1.05 G elements/s** (mmap-backed, zero-copy).
- **SIMD aggregation** delivers **4.49 G elements/s** for sum -- 3.97x faster than scalar.
- **Partition pruning** reduces scan time from **2.53 ms to 45.5 us** (55x speedup).
- **Parallel query** across 30 partitions is **1.93x faster** than sequential (13.08 vs 6.77 M rows/s).
- **Delta encoding** of timestamps runs at **5.59 G elements/s**.
- **LZ4 compression** of f64 columns runs at **18.22 GiB/s**.
- **OHLCV aggregation** processes **219.59 M ticks/s** for 1-second bars.
- **OrderBook delta application** handles **89.89 M deltas/s**.
- **Multi-partition GROUP BY** peaks at **61.05 M rows/s** with 30 partitions.
- **TSBS max-CPU-12h** query achieves **1.19 G rows/s** via partition pruning + BETWEEN optimization.
- **WAL deferred merge** reaches **8.09 M rows/s** -- 8.2x faster than sync WAL commit.
- **Filter pushdown** provides **1.64x** speedup over full scan for selective queries.

---

## Areas Needing Optimization

1. **ASOF JOIN**: Currently O(n*m) naive implementation -- **74 seconds** for 100K x 500K join.
   Needs sorted merge join or index-based lookup. Target: <100 ms.

2. **Compiled filters underperform interpreted**: Compiled filters are **1.5-3.5x slower** than
   hand-written interpreted loops. The closure-based compilation adds dispatch overhead.
   Consider generating native code or using vectorized column-at-a-time evaluation.

3. **Row-at-a-time writer**: The `TableWriter::write_row()` path (4.40 M rows/s for 1M rows)
   is significantly slower than batch write (18.80 M rows/s). Applications should prefer
   `write_batch()` or `write_batch_raw()` for bulk ingestion.

4. **WAL sync commit overhead**: Sync WAL commits (990 K rows/s) are 8x slower than deferred
   merge (8.09 M rows/s). Consider background merge as default for high-throughput workloads.

5. **SQL INSERT path**: At 50.52 K rows/s, SQL-based INSERT is the slowest ingestion path
   due to per-statement parse+plan overhead. Batch size tuning and prepared statements would help.

6. **Symbol map at scale**: Lookup throughput drops from 49.18 M/s (1K symbols) to 36.61 M/s
   (100K symbols) -- a 25% degradation. Hash table resizing or cache-friendly layouts needed.

7. **Full scan query throughput**: SELECT * at 6.15 M rows/s for 1M rows is bounded by
   row materialization cost. Column-oriented result sets (Arrow format) would eliminate this.

8. **ORDER BY + LIMIT**: TopK optimization helps for small K (top 10: 3.28 M rows/s) but
   degrades at K=10000 (2.01 M rows/s). Partial sort or streaming top-k heap could improve this.
