# ExchangeDB -- Rust Time-Series Database for Exchanges

## Overview

High-performance columnar time-series database written in Rust, optimized for exchange workloads:
tick data, OHLCV, order book snapshots, trades, and market events.

Inspired by QuestDB architecture, but leveraging Rust's zero-cost abstractions,
memory safety, and predictable latency (no GC pauses).

**Project statistics (as of March 2026):**

| Metric | Value |
|--------|-------|
| Lines of Rust | ~200,000 |
| Source files | ~361 |
| Crates | 6 (common, core, query, net, exchange, server) |
| SQL functions | 1,198+ |
| Aggregate kinds | 120+ |
| Data types | 41 |
| Cursor strategies | 124 |
| SQL features | Full DML/DDL, JOINs, window functions, CTEs, SAMPLE BY |

---

## Architecture (Modules)

```
exchange-db/
├── crates/
│   ├── core/              # Storage engine (Cairo analog)
│   │   ├── column.rs      # Column-oriented storage (fixed, variable, symbol)
│   │   ├── partition.rs   # Time-based partitioning (hour/day/month/year)
│   │   ├── partition_mgmt.rs # Partition management (detach/attach/squash)
│   │   ├── partition_lock.rs # Partition-level locking
│   │   ├── mmap.rs        # Memory-mapped file abstraction
│   │   ├── wal/           # Write-Ahead Log (segments, reader, writer, events)
│   │   ├── wal_writer.rs  # Dedicated WAL writer
│   │   ├── index/         # Bitmap indexes + symbol maps
│   │   ├── index_builder.rs # Index construction
│   │   ├── txn.rs         # Transaction management & scoreboard
│   │   ├── o3.rs          # Out-of-order data handling
│   │   ├── table.rs       # TableWriter, TableReader, TableBuilder, metadata
│   │   ├── meta_binary.rs # Binary metadata format
│   │   ├── engine.rs      # Storage engine coordinator
│   │   ├── compression.rs # Column compression (LZ4, delta, RLE, dictionary)
│   │   ├── snapshot.rs    # Point-in-time snapshots
│   │   ├── checkpoint.rs  # Checkpoint management
│   │   ├── recovery.rs    # Crash recovery
│   │   ├── pitr.rs        # Point-in-time recovery
│   │   ├── retention.rs   # Data retention policies
│   │   ├── ttl.rs         # Time-to-live cleanup
│   │   ├── vacuum.rs      # Space reclamation
│   │   ├── dedup.rs       # Row deduplication
│   │   ├── matview.rs     # Materialized views
│   │   ├── downsampling.rs # Automatic downsampling (OHLCV bars)
│   │   ├── simd.rs        # SIMD-accelerated operations
│   │   ├── simd_string.rs # SIMD string operations
│   │   ├── prefetch.rs    # Memory prefetch hints
│   │   ├── async_io.rs    # Async I/O (io_uring / kqueue)
│   │   ├── intern.rs      # String interning
│   │   ├── cursor.rs      # Storage-level cursor interface
│   │   ├── io_uring_reader.rs # Platform-aware I/O (io_uring on Linux, mmap fallback)
│   │   ├── sync.rs        # Synchronization primitives
│   │   ├── write_lock.rs  # Table-level write locks
│   │   ├── column_version.rs # Schema evolution / column versioning
│   │   ├── checksum.rs    # Partition checksums
│   │   ├── encryption.rs  # Encryption at rest
│   │   ├── resource.rs    # Resource management
│   │   ├── health.rs      # Health checks
│   │   ├── scheduler.rs   # Background task scheduling
│   │   ├── audit.rs       # Audit logging
│   │   ├── rbac/          # Role-Based Access Control (users, roles, permissions, store)
│   │   ├── cluster/       # Cluster management (nodes, router)
│   │   ├── consensus/     # Consensus protocol (Raft)
│   │   ├── replication/   # WAL-based replication (primary/replica)
│   │   ├── tiered/        # Tiered storage (hot/warm/cold)
│   │   ├── parquet/       # Parquet file format support
│   │   │   ├── thrift.rs  # Thrift compact protocol encoder for Parquet metadata
│   │   │   └── apache_writer.rs # Real Apache Parquet writer (PAR1 format)
│   │   ├── tenant.rs      # Multi-tenancy
│   │   ├── rls.rs         # Row-level security
│   │   └── metering.rs    # Usage metering
│   │
│   ├── query/             # Query engine (Griffin analog)
│   │   ├── parser.rs      # SQL parser (sqlparser-rs based)
│   │   ├── planner.rs     # Query planner
│   │   ├── optimizer.rs   # Query optimizer (partition pruning, predicate pushdown, index selection)
│   │   ├── executor.rs    # Main query executor
│   │   ├── cursor_executor.rs # Cursor-based execution pipeline
│   │   ├── pipeline.rs    # Execution pipeline builder
│   │   ├── plan.rs        # Query plan representation (120+ aggregate kinds)
│   │   ├── plan_cache.rs  # Prepared statement / plan caching
│   │   ├── batch.rs       # RecordBatch for vectorized execution
│   │   ├── record_cursor.rs # RecordCursor trait
│   │   ├── cursors/       # 124 cursor implementations (scan, filter, join, aggregate, window, ...)
│   │   ├── scalar.rs      # 1,198+ registered scalar functions (string, math, date/time, crypto, ...)
│   │   ├── functions.rs   # Function registry
│   │   ├── functions_extra.rs # Additional functions (finance, random, etc.)
│   │   ├── functions_compat.rs # PostgreSQL compatibility functions
│   │   ├── exchange_functions.rs # Exchange-domain scalar functions (VWAP, mid_price, spread)
│   │   ├── casts.rs       # Type cast functions
│   │   ├── value.rs       # CompactValue with Small String Optimization (SSO)
│   │   ├── sequence.rs    # Sequence support (CREATE SEQUENCE, nextval, currval, setval)
│   │   ├── window.rs      # Window function execution
│   │   ├── join.rs        # Join algorithms
│   │   ├── asof.rs        # ASOF JOIN implementation
│   │   ├── latest.rs      # LATEST ON implementation
│   │   ├── latest_indexed.rs # Index-driven LATEST ON
│   │   ├── parallel.rs    # Parallel partition scanning
│   │   ├── parallel_sort.rs # Parallel sorting (k-way merge)
│   │   ├── parallel_groupby.rs # Parallel GROUP BY
│   │   ├── vector_groupby.rs # Vectorized GROUP BY
│   │   ├── columnar.rs    # Columnar execution path
│   │   ├── compiled_filter.rs # Compiled filter expressions
│   │   ├── adaptive.rs    # Adaptive execution strategy selection
│   │   ├── catalog.rs     # System catalog (pg_catalog, information_schema)
│   │   ├── context.rs     # Query execution context
│   │   ├── memory.rs      # Memory budget management
│   │   ├── timeout.rs     # Query timeout support
│   │   ├── profiler.rs    # Query profiling (EXPLAIN ANALYZE)
│   │   ├── slow_log.rs    # Slow query logging
│   │   ├── spill.rs       # Disk spill for large sorts / GROUP BY
│   │   └── test_utils.rs  # Test utilities
│   │
│   ├── net/               # Network protocols (Cutlass analog)
│   │   ├── http/          # HTTP REST API (handlers, response, export, import, admin, diagnostics, rate_limit)
│   │   ├── pgwire/        # PostgreSQL wire protocol (handler, extended query, COPY)
│   │   ├── ilp/           # InfluxDB Line Protocol (TCP server, UDP server, parser, auth)
│   │   ├── ws/            # WebSocket (real-time streaming, handler)
│   │   ├── console/       # Built-in web console (SQL editor UI)
│   │   ├── auth.rs        # Authentication middleware (token, JWT)
│   │   ├── auth_routes.rs # Authentication HTTP routes
│   │   ├── oauth.rs       # OAuth 2.0 / OIDC integration
│   │   ├── service_account.rs # Service account management
│   │   ├── session.rs     # Session management
│   │   ├── pool.rs        # Connection pooling
│   │   ├── tls.rs         # TLS support
│   │   └── metrics.rs     # Prometheus metrics endpoint
│   │
│   ├── exchange/          # Exchange-specific extensions
│   │   ├── orderbook.rs   # Order book snapshot storage & reconstruction
│   │   ├── ohlcv.rs       # OHLCV aggregation engine
│   │   ├── tick.rs        # Tick data optimizations
│   │   └── market.rs      # Market data types
│   │
│   ├── common/            # Shared primitives
│   │   ├── types.rs       # Column types (41 types), Timestamp, PartitionBy
│   │   ├── error.rs       # Error types (ExchangeDbError)
│   │   ├── ringbuf.rs     # Lock-free ring buffers
│   │   ├── clock.rs       # High-resolution clocks
│   │   ├── hash.rs        # Hash functions (xxHash, FxHash)
│   │   ├── geo.rs         # Geospatial types
│   │   ├── ipv4.rs        # IPv4 type
│   │   └── decimal.rs     # Decimal types
│   │
│   └── server/            # Server binary
│       ├── main.rs        # Entry point, signal handling, config reload (SIGHUP)
│       ├── config.rs      # Configuration (TOML, env vars, CLI flags)
│       ├── log_rotation.rs # Log rotation
│       ├── bench_report.rs # Benchmark reporting
│       └── tsbs.rs        # TSBS compatibility
```

---

## Module Details

### 1. `core` -- Storage Engine

#### Column Storage (`core/column.rs`)
- **FixedColumn**: Fixed-size types (bool, i8, i16, i32, i64, f32, f64, u128/UUID, timestamp)
- **VarColumn**: Variable-length (String, Binary) -- data file (.d) + offset index (.i)
- **SymbolColumn**: Dictionary-encoded strings -- int ID + symbol map (ideal for ticker symbols)
- Null bitmap per column (.n file)
- Append-only write path for maximum throughput
- Compression: LZ4, delta encoding, RLE, dictionary encoding

#### Partitioning (`core/partition.rs`, `core/partition_mgmt.rs`)
- Strategies: `None`, `Hour`, `Day`, `Week`, `Month`, `Year`
- Directory per partition: `2024-03-01/`, `2024-03-01T14/`
- Automatic partition creation on write
- Partition pruning during query (skip irrelevant time ranges)
- Detach/attach partitions for cold storage
- Partition squashing (merge adjacent partitions)

#### Memory-Mapped Files (`core/mmap.rs`)
- `MmapRead` -- read-only mapping (column readers)
- `MmapReadWrite` -- append + read (column writers)
- `MmapAnon` -- anonymous mapping (temp buffers)
- Configurable page sizes (4KB, 2MB huge pages)
- madvise hints: `MADV_SEQUENTIAL`, `MADV_WILLNEED`, `MADV_DONTNEED`
- Prefetch support (`core/prefetch.rs`)

#### Write-Ahead Log (`core/wal/`)
- Per-table WAL segments
- Event types: Data, DDL, Truncate
- WAL -> main table merge via background task
- Sequencer for transaction ordering
- Configurable: sync/async commit modes
- WAL-based replication to replicas

#### Indexes (`core/index/`)
- **BitmapIndex**: Key -> sorted row ID list (for symbol/enum columns)
- **SymbolMap**: String <-> int ID bidirectional mapping
- Key file (.k) + value file (.v), both mmap'd
- Concurrent readers with lock-free access
- Index builder for bulk construction

#### Transactions (`core/txn.rs`)
- `_txn` file: partition list, row counts, min/max timestamps
- Scoreboard: tracks active readers to prevent premature cleanup
- MVCC-like isolation: readers see consistent snapshot

#### Enterprise Features
- **RBAC** (`core/rbac/`): Users, roles, permissions, persistent store
- **Replication** (`core/replication/`): WAL shipping (primary -> replica), failover
- **Cluster** (`core/cluster/`): Multi-node coordination, query routing
- **Consensus** (`core/consensus/`): Raft protocol
- **Tiered Storage** (`core/tiered/`): Hot/warm/cold data movement
- **Encryption** (`core/encryption.rs`): Encryption at rest (ChaCha20-Poly1305)
- **Row-Level Security** (`core/rls.rs`): Row-level access control
- **Multi-Tenancy** (`core/tenant.rs`): Namespace isolation
- **Metering** (`core/metering.rs`): Usage tracking
- **Audit** (`core/audit.rs`): Audit trail for security events

### 2. `query` -- Query Engine

#### SQL Support
```sql
-- Time-series queries
SELECT symbol, avg(price), max(volume)
FROM trades
WHERE timestamp BETWEEN '2024-01-01' AND '2024-03-01'
  AND symbol = 'BTC/USD'
SAMPLE BY 1h;

-- OHLCV aggregation
SELECT
  first(price) as open, max(price) as high,
  min(price) as low, last(price) as close,
  sum(volume) as volume
FROM ticks
SAMPLE BY 1m ALIGN TO CALENDAR;

-- AS OF join (point-in-time)
SELECT t.*, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q ON (t.symbol = q.symbol);

-- LATEST ON
SELECT * FROM trades
LATEST ON timestamp PARTITION BY symbol;

-- Window functions
SELECT symbol, price,
  avg(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) as ma10
FROM trades;

-- CTEs
WITH recent AS (SELECT * FROM trades WHERE timestamp > '2024-01-01')
SELECT symbol, count(*) FROM recent GROUP BY symbol;

-- CASE WHEN, IN, LIKE, CAST, BETWEEN
SELECT CASE WHEN price > 100 THEN 'high' ELSE 'low' END as tier
FROM trades
WHERE symbol IN ('BTC/USD', 'ETH/USD') AND side LIKE 'B%';

-- DDL
CREATE TABLE trades (
  timestamp TIMESTAMP, symbol SYMBOL, price DOUBLE, volume DOUBLE
) TIMESTAMP(timestamp) PARTITION BY DAY;
ALTER TABLE trades ADD COLUMN fee DOUBLE;
ALTER TABLE trades DROP COLUMN fee;

-- Partition management
ALTER TABLE trades DETACH PARTITION '2024-01-01';
ALTER TABLE trades ATTACH PARTITION '2024-01-01';
ALTER TABLE trades SQUASH PARTITIONS '2024-01-01' '2024-01-02';

-- PIVOT, MERGE, INSERT ON CONFLICT
-- SHOW TABLES, SHOW COLUMNS, SHOW CREATE TABLE, DESCRIBE
-- BEGIN / COMMIT / ROLLBACK (compatibility)
-- EXPLAIN, EXPLAIN ANALYZE
-- INSERT INTO ... SELECT, TRUNCATE TABLE
-- CREATE/DROP USER, CREATE/DROP ROLE, GRANT/REVOKE
-- CREATE/DROP/REFRESH MATERIALIZED VIEW
```

#### Execution Model
- Cursor-based (pull model) with 124 cursor implementations
- Vectorized execution on record batches
- Parallel scan across partitions (rayon)
- Adaptive strategy selection (row-at-a-time, vectorized, columnar, parallel)
- Spill to disk for large sorts and GROUP BY
- Query timeout and memory budget
- Plan caching for prepared statements

#### Cursor Types (124)
Scan: `scan`, `filtered_scan`, `column_scan`, `null_scan`, `reverse_scan`, `index_scan`, `symbol_filter_scan`, `timestamp_range_scan`, `sampled_scan`, `parallel_scan`, `async_scan`, `page_frame`

Join: `hash_join`, `nested_loop_join`, `sort_merge_join`, `asof_join`, `cross_join`, `semi_join`, `anti_join`, `mark_join`, `band_join`, `index_join`, `broadcast_join`, `window_join`

Aggregate: `aggregate`, `group_by_hash`, `group_by_sorted`, `streaming_aggregate`, `incremental_aggregate`, `parallel_aggregate`, `approximate_aggregate`, `rollup`, `cube`, `pivoted_aggregate`, `having_filter`

Transform: `filter`, `deferred_filter`, `sort`, `merge_sort`, `topk`, `distinct`, `limit`, `project`, `rename`, `type_cast`, `expression`, `case_when`, `coalesce`, `nullif`, `row_id`, `constant`

Set: `union`, `intersect`, `except`, `concat`

Window: `window`, `fill`, `sample_by`, `latest_by`

Control: `empty`, `memory`, `values`, `generate_series`, `explain`, `cache`, `buffer`, `tee`, `spill`, `timeout`, `progress`, `stats`, `debug`, `rate_limit`, `builder`, `count_only`

#### Functions
- **1,198+ scalar functions**: String (length, substr, upper, lower, trim, replace, concat, regexp, ...),
  Math (abs, ceil, floor, round, sqrt, log, pow, sin, cos, ...), Date/Time (now, dateadd, datediff,
  date_trunc, to_timestamp, extract, ...), Conditional (coalesce, nullif, greatest, least, decode, ...),
  Type casts, Crypto (md5, sha256), Random (rnd_int, rnd_str, ...), System functions
- **120+ aggregate kinds**: Sum, Avg, Min, Max, Count, First, Last, StdDev, Variance, Median,
  Percentile, VWAP, EMA, SMA, RSI, Bollinger Bands, ATR, and per-type variants

### 3. `net` -- Network Layer

| Protocol | Port | Use Case |
|----------|------|----------|
| HTTP/REST | 9000 | Query API, health, metrics, admin, export/import |
| WebSocket | 9000 | Real-time market data streaming |
| PostgreSQL | 8812 | Compatible with psql, DBeaver, Grafana |
| ILP/TCP | 9009 | High-throughput data ingestion |
| ILP/UDP | 9009 | Fire-and-forget ingestion |
| Web Console | 9000 | Built-in SQL editor UI |

### 4. `exchange` -- Domain-Specific

- **Order book** (`orderbook.rs`): Store L2/L3 snapshots, reconstruct book at any timestamp
- **OHLCV** (`ohlcv.rs`): Materialized views with incremental updates
- **Tick** (`tick.rs`): Nanosecond precision, delta encoding for price/size
- **Market** (`market.rs`): Market data types and structures

---

## Key Design Decisions

### Rust Advantages Over Java (QuestDB)
| Aspect | QuestDB (Java) | ExchangeDB (Rust) |
|--------|---------------|-------------------|
| GC Pauses | Stop-the-world GC pauses (ms-s) | No GC, deterministic drops |
| Memory | Off-heap via Unsafe (workaround) | Native control, no JNI |
| Concurrency | Manual lock-free via Unsafe | `Send`/`Sync` compile-time safety |
| SIMD | JNI or JIT | Native intrinsics, portable_simd |
| Latency | p99 spikes from GC | Predictable p99 |
| Binary size | JVM + JAR (~200MB) | Single static binary (~10MB) |

### Data Format on Disk
```
table-name/
├── _meta              # Table metadata (JSON or binary format)
├── _txn               # Transaction file (partition list, row counts)
├── _cv                # Column versions (schema evolution)
├── _wal/
│   ├── wal-0/         # WAL segment 0
│   │   ├── _events    # Event log
│   │   ├── col_a.d    # Column data
│   │   └── col_b.d
│   └── wal-1/
├── 2024-03-01/        # Day partition
│   ├── timestamp.d    # Designated timestamp column
│   ├── price.d        # Fixed column (f64)
│   ├── volume.d       # Fixed column (f64)
│   ├── symbol.d       # Symbol column (i32 IDs)
│   ├── symbol.k       # Symbol index keys
│   ├── symbol.v       # Symbol index values
│   └── side.d         # Enum column (buy/sell)
└── 2024-03-02/
```

---

## Dependencies

| Crate | Purpose |
|-------|---------|
| `memmap2` | Memory-mapped files |
| `crossbeam` | Lock-free structures, channels |
| `tokio` | Async runtime |
| `sqlparser` | SQL parsing |
| `rayon` | Parallel query execution |
| `criterion` | Benchmarking |
| `serde` / `serde_json` | Serialization |
| `tracing` | Structured logging |
| `xxhash-rust` | Fast hashing |
| `pgwire` | PostgreSQL wire protocol |
| `axum` / `hyper` | HTTP server |
| `tokio-tungstenite` | WebSocket |
| `regex` | Regular expressions |
| `lz4_flex` | LZ4 compression |

---

## Development Phases

### Phase 1: Core Storage (MVP) -- COMPLETE
- [x] Column types: bool, i8-i64, f32, f64, timestamp (nanos), symbol, varchar, binary, uuid
- [x] Append-only TableWriter with TableBuilder
- [x] TableReader with mmap
- [x] Time-based partitioning (Hour/Day/Week/Month/Year)
- [x] Metadata files (_meta, _txn, _cv)
- [x] WAL with segments, merge, recovery
- [x] Out-of-order insert handling (O3)
- [x] Bitmap indexes for symbol columns
- [x] Compression (LZ4, delta, RLE, dictionary)

### Phase 2: Query Engine -- COMPLETE
- [x] SQL parser (sqlparser-rs)
- [x] Query planner and optimizer
- [x] SELECT/WHERE/ORDER BY/GROUP BY/HAVING/DISTINCT
- [x] SAMPLE BY (time bucketing) with FILL and ALIGN
- [x] Aggregate functions (120+ kinds incl. financial)
- [x] 1,198+ scalar functions
- [x] Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.)
- [x] All JOINs (INNER, LEFT, RIGHT, FULL OUTER, CROSS, ASOF, SEMI, ANTI, MARK, BAND)
- [x] LATEST ON, CTEs, subqueries, UNION/INTERSECT/EXCEPT
- [x] CASE WHEN, IN, LIKE, CAST, BETWEEN, IS NULL
- [x] EXPLAIN / EXPLAIN ANALYZE
- [x] PIVOT, MERGE, INSERT ON CONFLICT

### Phase 3: 124 Cursor Implementations -- COMPLETE
- [x] Full scan cursors (forward, reverse, index, parallel, async)
- [x] Join cursors (hash, nested loop, sort-merge, ASOF, cross, semi, anti, mark, band, index, broadcast, window)
- [x] Aggregate cursors (hash, sorted, streaming, incremental, parallel, approximate, rollup, cube)
- [x] Transform cursors (filter, sort, merge-sort, topk, distinct, limit, project, expression, cast)
- [x] Set operation cursors (union, intersect, except, concat)
- [x] Control cursors (cache, buffer, tee, spill, timeout, progress, stats, debug)

### Phase 4: Network & Protocols -- COMPLETE
- [x] HTTP REST API (query, health, tables, export, import, diagnostics, admin)
- [x] PostgreSQL wire protocol (simple + extended query, COPY)
- [x] ILP ingestion (TCP + UDP)
- [x] WebSocket streaming
- [x] Built-in web console (SQL editor UI)
- [x] Prometheus metrics endpoint
- [x] Authentication (token, JWT/OAuth)
- [x] TLS support
- [x] Rate limiting
- [x] Connection pooling

### Phase 5: Enterprise Features -- COMPLETE
- [x] RBAC (users, roles, permissions, table-level + column-level)
- [x] WAL-based replication (primary -> replica, failover)
- [x] Cluster management and query routing
- [x] Tiered storage (hot/warm/cold)
- [x] Encryption at rest
- [x] Multi-tenancy
- [x] Row-level security
- [x] Audit logging
- [x] Service accounts and OAuth/OIDC
- [x] Materialized views

### Phase 6: Performance & Operations -- COMPLETE
- [x] SIMD-accelerated operations (aggregation, string search)
- [x] Parallel execution (partition scan, sort, GROUP BY)
- [x] Adaptive execution strategy selection
- [x] Spill-to-disk for large operations
- [x] Query profiling (EXPLAIN ANALYZE)
- [x] Slow query logging
- [x] Memory budget per query
- [x] Plan caching
- [x] Snapshot and checkpoint support
- [x] Point-in-time recovery
- [x] Log rotation
- [x] Hot config reload (SIGHUP)
- [x] Health checks and diagnostics endpoint

### Remaining Work
- [ ] Full test suite recovery (some test compilation issues to resolve)
- [ ] Apache Parquet read/write (real format, not XPQT)
- [ ] io_uring async I/O (Linux)
- [ ] HTTP/2 / gRPC
- [ ] Automatic failover detection (consensus-based)
- [ ] Per-tenant resource quotas
- [ ] JIT filter compilation (cranelift)
- [ ] CPU affinity for worker threads

---

## Quick Start

### Build from Source

```bash
cargo build --release
./target/release/exchange-db server
./target/release/exchange-db server --config exchange-db.toml
./target/release/exchange-db server --bind 127.0.0.1:9000 --data-dir /var/lib/exchangedb
```

### Docker

```bash
docker compose up -d

# Or directly:
docker build -t exchangedb .
docker run -d --name exchangedb \
  -p 9000:9000 -p 8812:8812 -p 9009:9009 \
  -v exchangedb-data:/data exchangedb
```

### CLI Commands

```bash
exchange-db sql "SELECT * FROM trades LIMIT 10"
exchange-db import --table trades --file trades.csv
exchange-db tables
exchange-db info trades
```

---

## Configuration Reference

ExchangeDB loads configuration in the following priority order (highest wins):
1. CLI flags (`--bind`, `--data-dir`)
2. Environment variables (`EXCHANGEDB_*`)
3. Config file (`exchange-db.toml`)
4. Built-in defaults

### Config File (`exchange-db.toml`)

```toml
[server]
data_dir = "./data"
log_level = "info"

[http]
bind = "0.0.0.0:9000"
enabled = true

[pgwire]
bind = "0.0.0.0:8812"
enabled = true

[ilp]
bind = "0.0.0.0:9009"
enabled = true
batch_size = 1000

[storage]
wal_enabled = true
wal_max_segment_size = "64MB"
default_partition_by = "day"
mmap_page_size = "4KB"

[retention]
enabled = false
max_age = "30d"
check_interval = "1h"

[performance]
query_parallelism = 0        # 0 = auto (num_cpus)
writer_commit_mode = "async"  # "sync" or "async"

[security]
auth_enabled = false
token = ""                    # Static token auth
oauth_issuer = ""             # OAuth 2.0 / OIDC issuer URL
tls_cert = ""                 # TLS certificate path
tls_key = ""                  # TLS key path

[replication]
mode = "standalone"           # "standalone", "primary", "replica"
primary_addr = ""             # Address of primary (replica mode)
```

### Environment Variables

| Variable                        | Description                    | Default         |
|---------------------------------|--------------------------------|-----------------|
| `EXCHANGEDB_DATA_DIR`           | Root data directory            | `./data`        |
| `EXCHANGEDB_LOG_LEVEL`          | Log level (trace/debug/info)   | `info`          |
| `EXCHANGEDB_HTTP_BIND`          | HTTP server bind address       | `0.0.0.0:9000`  |
| `EXCHANGEDB_HTTP_ENABLED`       | Enable HTTP server             | `true`          |
| `EXCHANGEDB_PGWIRE_BIND`       | PostgreSQL wire protocol bind  | `0.0.0.0:8812`  |
| `EXCHANGEDB_PGWIRE_ENABLED`    | Enable pgwire server           | `true`          |
| `EXCHANGEDB_ILP_BIND`          | ILP ingestion bind address     | `0.0.0.0:9009`  |
| `EXCHANGEDB_ILP_ENABLED`       | Enable ILP ingestion           | `true`          |
| `EXCHANGEDB_ILP_BATCH_SIZE`    | ILP batch size                 | `1000`          |
| `EXCHANGEDB_WAL_ENABLED`       | Enable write-ahead log         | `true`          |
| `EXCHANGEDB_QUERY_PARALLELISM` | Query thread count (0 = auto)  | `0`             |
| `EXCHANGEDB_WRITER_COMMIT_MODE`| Writer commit mode             | `async`         |

### Size and Duration Formats

Byte sizes: `4KB`, `64MB`, `1GB`, `2TB` (or plain integers for bytes).

Durations: `30s`, `5m`, `1h`, `7d`, `2w`.

---

## API Endpoints Reference

### HTTP REST API (default port 9000)

| Method | Endpoint            | Description                              |
|--------|---------------------|------------------------------------------|
| GET    | `/`                 | Built-in web console (SQL editor UI)     |
| GET    | `/health`           | Health check (status, version, uptime)   |
| GET    | `/api/v1/health`    | Health check (aliased)                   |
| POST   | `/api/v1/query`     | Execute SQL query, returns JSON results  |
| POST   | `/api/v1/write`     | Write data (ILP format over HTTP)        |
| GET    | `/api/v1/tables`    | List all tables                          |
| GET    | `/api/v1/tables/:name` | Table metadata (schema, row count)    |
| GET    | `/api/v1/export`    | Export query results as CSV              |
| POST   | `/api/v1/import`    | Import CSV data                          |
| GET    | `/api/v1/diagnostics` | System diagnostics (version, memory, storage) |
| GET    | `/metrics`          | Prometheus metrics                       |
| POST   | `/api/v1/admin/*`   | Admin operations                         |

### Web Console

ExchangeDB includes a built-in web console served at `GET /`, similar to QuestDB's
built-in console. The console is a single-page application with all HTML, CSS, and
JavaScript embedded as const strings in Rust (no external static files required).

Features:
- Dark-themed SQL editor with syntax highlighting placeholder
- Table browser sidebar (auto-loads from `/api/v1/tables`)
- Query results rendered as a scrollable table
- Keyboard shortcut: `Ctrl+Enter` / `Cmd+Enter` to execute
- Timing display for query execution
- Error display with red highlighting

Source: `crates/net/src/console/mod.rs`

### Diagnostics Endpoint

`GET /api/v1/diagnostics` returns comprehensive system information as JSON:

```json
{
    "version": "0.1.0",
    "rust_version": "1.85",
    "os": "darwin",
    "arch": "aarch64",
    "pid": 12345,
    "uptime_secs": 3600,
    "memory": { "rss_bytes": 100000000, "heap_bytes": 50000000 },
    "storage": { "data_dir": "./data", "disk_free_bytes": 500000000, "tables": 5, "total_rows": 1000000 },
    "connections": { "http": 10, "pgwire": 3, "ilp": 2 },
    "wal": { "pending_segments": 0, "applied_segments": 15 },
    "config": { "http_port": 9000, "pg_port": 8812, "ilp_port": 9009 }
}
```

Source: `crates/net/src/http/diagnostics.rs`

### PostgreSQL Wire Protocol (default port 8812)

Connect with any PostgreSQL client:
```bash
psql -h localhost -p 8812 -d exchangedb
```

Supported: Simple Query Protocol, Extended Query Protocol, Prepared Statements, COPY IN/OUT.

### InfluxDB Line Protocol (default port 9009)

High-throughput data ingestion over TCP or UDP:
```
trades,symbol=BTC/USD price=67543.21,volume=1.5 1710000000000000000
trades,symbol=ETH/USD price=3421.50,volume=10.0 1710000000000000000
```

---

## Operational Features

### Hot Config Reload (SIGHUP)

On Unix systems, sending `SIGHUP` to the ExchangeDB process triggers a live
reload of the configuration file. Runtime-safe settings (e.g. log level) are
applied without restarting:

```bash
kill -HUP $(pidof exchange-db)
```

Source: `crates/server/src/main.rs`

### Log Rotation

`RotatingLog` provides a simple file-based log rotation mechanism:
- Writes to `<prefix>.log` in a configurable directory
- Automatically rotates when the file exceeds `max_size` bytes
- Keeps at most `max_files` rotated files (`.1`, `.2`, ...)
- Thread-safe via internal `Mutex`

Source: `crates/server/src/log_rotation.rs`

### Performance Benchmark Results

_Placeholder: benchmark results will be added after running the full TSBS suite._

```
TODO: Run and document:
- Insert throughput (rows/sec for ILP ingestion)
- Query latency (p50/p99 for SAMPLE BY, GROUP BY, ASOF JOIN)
- Parallel scan speedup (1 vs N partitions)
- Memory usage under load
- Comparison vs QuestDB on equivalent workloads
```
