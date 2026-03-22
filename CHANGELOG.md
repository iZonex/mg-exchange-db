# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-03-22

### Exchange Functions
- 21 new exchange-specific SQL functions:
  - Balance engine: `liquidation_price()`, `unrealized_pnl()`, `margin_required()`,
    `funding_payment()`, `fee_amount()`, `balance_available()`, `margin_ratio()`, `position_value()`
  - Extended OHLCV: `taker_buy_volume()`, `taker_sell_volume()`, `quote_volume()`,
    `delta()`, `delta_pct()`, `trade_count()`
  - Order state: `order_fill_pct()`, `is_fully_filled()`, `remaining_qty()`,
    `effective_price()`, `slippage()`

### Reliability
- Split-brain prevention with fencing tokens and monotonic epochs
- Automated replica re-sync (`exchange-db replication resync`)
- Replication lag monitoring (lag_bytes, lag_ms) with Prometheus gauges
- Encryption key rotation (`exchange-db key-rotate`)

### Testing
- Fuzz testing: cargo-fuzz targets for SQL parser, ILP parser, WAL codec
- Load testing: `exchangedb-loadtest` binary (concurrent readers + writers)
- Chaos testing: 6 tests (kill-during-write, corruption, disk full, concurrent stress)
- Lock contention profiling (N=1-16 threads)

### Infrastructure
- Helm chart for Kubernetes (StatefulSet, replication, ServiceMonitor)
- Production checklist: 67/68 (99%)

---

## [0.1.1] - 2026-03-21

### Performance
- Mmap cache: reuse memory-mapped file handles across queries (eliminates repeated mmap/munmap)
- Table registry: keep tables open in memory for zero-overhead scanning
- Optimizer skip: bypass expensive optimization for simple queries
- Limit pushdown: scan 1 partition instead of all for LIMIT queries
- Result: **100-200x speedup** (SELECT * LIMIT 25: 2ms → 11µs, KDB+ competitive)

### Security
- Path traversal fix: reject `../` and absolute paths in table names
- Argon2 password hashing for service accounts (replacing SHA-256)
- Auth JSON injection fix: properly escape user input in auth responses
- Diagnostics endpoint requires authentication

### CLI
- New commands: `config show/validate/generate`, `check all/wal/partitions/metadata/disk-usage`,
  `replication status/promote/demote`, `debug wal-inspect/partition-info/column-dump/diagnostics`,
  `compact`, `status`, `version`

### Web Console
- Server-side query timing (not HTTP round-trip)

### Code Quality
- Zero compiler warnings across all 6 crates
- Zero clippy warnings (all lints clean)

### Documentation
- CLI reference, docs index, production checklist
- GitHub community files (LICENSE, CoC, SECURITY, CONTRIBUTING)
- Issue templates, PR template, CODEOWNERS, dependabot

---

## [0.1.0] - 2026-03-21

### Added

#### Storage Engine
- Column-oriented storage engine with memory-mapped I/O (`memmap2`)
- 41 data types: bool, i8–i64, f32, f64, timestamp (nanos), symbol, varchar,
  binary, UUID, date, char, IPv4, long128, long256, geohash, decimal variants,
  geospatial, array, and more
- Time-based partitioning: None, Hour, Day, Week, Month, Year
- Write-Ahead Log (WAL) with segments, merge, and crash recovery
- Bitmap indexes for symbol columns with dictionary-encoded symbol maps
- Transaction management with scoreboard (MVCC-like snapshot isolation)
- Out-of-order (O3) data insertion handling
- Compression: LZ4, delta encoding, RLE, dictionary encoding
- Partition management: detach, attach, squash
- Tiered storage: hot / warm / cold data movement
- Snapshots, checkpoints, and point-in-time recovery (PITR)
- Data retention policies with TTL
- Vacuum (space reclamation) and row deduplication
- Real Apache Parquet writer (PAR1 files readable by Spark, DuckDB, Pandas)
- Thrift compact protocol encoder for Parquet metadata
- io_uring reader abstraction with mmap fallback on non-Linux platforms

#### SQL Engine
- SQL parser built on `sqlparser-rs`
- Query planner, optimizer, and executor with 124 cursor-based strategies
- DDL: CREATE/ALTER/DROP TABLE, CREATE/DROP MATERIALIZED VIEW, TRUNCATE
- ALTER TABLE: ADD/DROP/RENAME/SET TYPE COLUMN, DETACH/ATTACH/SQUASH PARTITION
- DML: INSERT, INSERT INTO...SELECT, INSERT ON CONFLICT, UPDATE, DELETE, MERGE, COPY TO/FROM
- SELECT: WHERE, ORDER BY, GROUP BY (GROUPING SETS, ROLLUP, CUBE), HAVING,
  DISTINCT, LIMIT/OFFSET, CTEs (WITH), subqueries, set operations
- 10 JOIN types: INNER, LEFT, RIGHT, FULL OUTER, CROSS, ASOF, LATERAL, SEMI, ANTI, MARK
- Time-series extensions: SAMPLE BY with FILL (NONE/NULL/PREV/LINEAR/constant),
  LATEST ON...PARTITION BY, ALIGN TO CALENDAR
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTH_VALUE,
  NTILE, PERCENT_RANK, CUME_DIST
- CASE WHEN, PIVOT/UNPIVOT, EXPLAIN/EXPLAIN ANALYZE
- SHOW TABLES/COLUMNS/CREATE TABLE, DESCRIBE
- BEGIN / COMMIT / ROLLBACK (compatibility)
- `generate_series()`, `long_sequence()`

#### Functions & Aggregates
- 1,198+ scalar functions: string, math, date/time, conditional, crypto, random, system
- 120+ aggregate kinds including financial: VWAP, EMA, SMA, WMA, RSI, MACD,
  Bollinger Bands, ATR, Drawdown
- Statistical aggregates: StdDev, Variance, Median, Percentile, Corr, Covariance
- Approximate aggregates (ApproxCountDistinct)
- Kahan/Neumaier compensated summation (Ksum, Nsum)
- Exchange-domain functions: `ohlcv_vwap`, `mid_price`, `spread`,
  `tick_delta_encode`, `tick_delta_decode`
- Full type cast system across all 41 types

#### Cursor Engine
- 124 cursor implementations: scan, join, aggregate, transform, set-op, control
- RecordBatch-based vectorized execution
- Adaptive execution strategy selection (row, vectorized, columnar, parallel)
- Spill-to-disk for large sorts and GROUP BY exceeding memory budget

#### Network & Protocols
- HTTP REST API: query, health, tables, export (CSV), import (CSV),
  diagnostics, admin, write (ILP over HTTP)
- PostgreSQL wire protocol: simple + extended query, prepared statements, COPY IN/OUT
- ILP ingestion: TCP + UDP servers with authentication
- WebSocket real-time streaming
- Built-in web console (SQL editor UI, dark theme, embedded HTML/CSS/JS)
- Prometheus metrics endpoint (`/metrics`)
- NDJSON streaming for large results

#### Security & Access Control
- RBAC: users, roles, permissions (table-level + column-level)
- CREATE/DROP USER, CREATE/DROP ROLE, GRANT/REVOKE SQL support
- Token authentication
- OAuth 2.0 / OIDC with JWT validation
- Service accounts
- Row-level security (RLS)
- Encryption at rest (ChaCha20-Poly1305)
- Audit logging

#### Replication & Clustering
- WAL-based replication (primary → replica)
- Failover promote/demote (manual)
- Cluster management and query routing
- Raft consensus protocol (basic)

#### Performance
- SIMD-accelerated aggregation and string operations
- Parallel partition scanning (rayon)
- Parallel sort with k-way merge
- Parallel and vectorized GROUP BY
- Columnar execution path
- String interning and memory prefetch hints
- Memory budget per query with query timeout
- Query profiling (EXPLAIN ANALYZE) and slow query logging
- Plan caching for prepared statements
- Async I/O framework (io_uring / kqueue stubs)
- Compiled filter expressions

#### Operations
- Connection pooling, session management, rate limiting (per-IP)
- TLS support for all protocols
- Multi-tenancy with namespace isolation (X-Tenant-ID header)
- Usage metering per tenant
- Materialized views: CREATE, DROP, REFRESH
- Partition checksums
- Hot config reload (SIGHUP)
- Log rotation
- Sequence support: CREATE/DROP SEQUENCE, nextval(), currval(), setval()
- Docker support (Dockerfile + docker-compose.yml)
- Health checks and diagnostics endpoint

#### Project
- 6-crate workspace: common, core, query, net, exchange, server
- ~200,000 lines of Rust across ~361 source files
- ~26,500 passing tests
- Comprehensive documentation: architecture, SQL reference, API reference,
  operations guide, security guide, migration guide, troubleshooting
- CI/CD: GitHub Actions for check, test, clippy, fmt, Docker build, release
- Benchmark suite (Criterion) with TSBS compatibility

---

**Stats at v0.1.0:**

| Metric | Value |
|--------|-------|
| Lines of Rust | ~200,000 |
| Source files | ~361 |
| Crates | 6 |
| Data types | 41 |
| SQL functions | 1,198+ |
| Aggregate kinds | 120+ |
| Cursor strategies | 124 |
| Tests | ~26,500 |
| Batch write | 18.80 M rows/s |
| Column read | 590.70 M elements/s |
| SIMD aggregation | 4.49 G elements/s |

[0.1.2]: https://github.com/iZonex/mg-exchange-db/releases/tag/v0.1.2
[0.1.1]: https://github.com/iZonex/mg-exchange-db/releases/tag/v0.1.1
[0.1.0]: https://github.com/iZonex/mg-exchange-db/releases/tag/v0.1.0
