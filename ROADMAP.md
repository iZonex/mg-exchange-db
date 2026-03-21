# ExchangeDB -- Roadmap to QuestDB Parity

## Scale (Updated March 2026 -- Wave 25)

| Metric | QuestDB | ExchangeDB | Coverage |
|--------|---------|------------|----------|
| Code | 1,428,324 lines Java | ~200,000 lines Rust | ~14% (with Rust compactness ~30-35%) |
| SQL functions | 1,046+ | 1,198+ | **114%** |
| Data types | 41 | 41 | **100%** |
| Cursor/execution strategies | 124 | 124 | **100%** |
| HTTP processors | 30+ | 14+ | ~47% |
| Background jobs | 18 | 14+ | ~78% |
| File formats | 15+ | 10 | ~67% |
| Configuration | 200+ params | ~50 | ~25% |

---

## Milestone 1: SQL Completeness -- ~95% DONE

### 1.1 SQL Operators

| Feature | Status | Priority |
|---------|--------|----------|
| `CASE WHEN ... THEN ... ELSE ... END` | DONE | P0 |
| `IN (value_list)` / `NOT IN` | DONE | P0 |
| `IN (subquery)` / `EXISTS (subquery)` | DONE | P0 |
| `IS NULL` / `IS NOT NULL` | DONE | P0 |
| `LIKE` / `ILIKE` pattern matching | DONE | P0 |
| `CAST(x AS type)` | DONE | P0 |
| `INSERT INTO ... SELECT` | DONE | P0 |
| `COALESCE(a, b, c, ...)` N-arg | DONE | P0 |
| `BETWEEN` symmetry edge cases | DONE | P1 |
| `TRUNCATE TABLE` | DONE | P1 |
| `SHOW TABLES` / `SHOW COLUMNS` | DONE | P1 |
| `DESCRIBE table` | DONE | P1 |
| `SHOW CREATE TABLE` | DONE | P1 |
| Correlated subqueries | DONE | P1 |
| `WITH RECURSIVE` (recursive CTEs) | DONE | P1 |
| `MERGE` / `ON CONFLICT` | DONE | P2 |
| `CREATE SEQUENCE` / `nextval` / `currval` | DONE | P2 |
| `CALL` statements | Not implemented | P3 |

### 1.2 Expressions

| Feature | Status | Priority |
|---------|--------|----------|
| Arithmetic in SELECT: `price * volume` | DONE | P0 |
| Arithmetic in WHERE: `price * 1.1 > 100` | DONE | P0 |
| String concatenation: `a \|\| b` | DONE | P0 |
| Column aliases: `SELECT price AS p` | DONE | P0 |
| Qualified column refs: `t.price` | DONE | P1 |
| Nested function calls: `round(avg(price), 2)` | DONE | P1 |
| Type coercion in expressions | DONE | P1 |
| Null propagation in arithmetic | DONE | P1 |

### 1.3 JOIN Improvements

| Feature | Status | Priority |
|---------|--------|----------|
| RIGHT JOIN | DONE | P1 |
| FULL OUTER JOIN | DONE | P1 |
| CROSS JOIN | DONE | P1 |
| NATURAL JOIN | Not implemented | P2 |
| Self-join | DONE | P1 |
| Multi-table JOIN (3+ tables) | DONE | P1 |
| SEMI JOIN / ANTI JOIN | DONE | P1 |
| BAND JOIN / MARK JOIN | DONE | P2 |
| Index-driven JOIN | DONE | P2 |
| Sort-merge JOIN | DONE | P2 |
| Async join execution | Not implemented | P3 |

### 1.4 SAMPLE BY Improvements

| Feature | Status | Priority |
|---------|--------|----------|
| `SAMPLE BY 1h FILL(NONE)` | DONE | P0 |
| `SAMPLE BY 1h FILL(NULL)` | DONE | P0 |
| `SAMPLE BY 1h FILL(PREV)` | DONE | P0 |
| `SAMPLE BY 1h FILL(LINEAR)` | DONE | P1 |
| `SAMPLE BY 1h FILL(value)` | DONE | P1 |
| `SAMPLE BY 1h ALIGN TO CALENDAR` | DONE | P0 |
| `SAMPLE BY 1h ALIGN TO FIRST OBSERVATION` | DONE | P1 |

---

## Milestone 2: Functions -- **114% DONE** (exceeds QuestDB)

### 2.1 Categories

| Category | QuestDB | ExchangeDB | Status |
|----------|---------|------------|--------|
| **Scalar functions** | 600+ | 1,198+ | **200%** |
| **Aggregate (group by)** | 213 | 120+ | ~56% |
| **Cast functions** | 226 | ~50 | ~22% |
| **Math** | 112 | ~60 | ~54% |
| **String** | 59 | ~50 | ~85% |
| **Date/Time** | 64 | ~40 | ~63% |
| **Random** | 43 | ~15 | ~35% |
| **Regex** | 19 | ~10 | ~53% |
| **Finance** | 6 | 15+ (VWAP, EMA, SMA, RSI, ATR, etc.) | **250%** |
| **Exchange-domain** | 0 | 10+ (ohlcv_vwap, mid_price, spread, tick_delta) | N/A |
| **Window** | 34 | ~15 | ~44% |
| **System/Catalog** | 97 | ~20 | ~21% |

### 2.2 Remaining high-priority functions

```
-- More random data generators needed
rnd_geohash(), rnd_bin()

-- System catalog functions (for psql/Grafana compatibility)
More pg_catalog tables and functions
information_schema completeness

-- Additional cast permutations between all 41 types
```

---

## Milestone 3: Query Execution Engine -- **100% DONE**

### 3.1 Execution Model

| Feature | Status | Priority |
|---------|--------|----------|
| Row-at-a-time execution | DONE | - |
| Columnar/vectorized execution | DONE | P0 |
| Page frame execution (batch of rows) | DONE | P0 |
| Lazy/streaming results (RecordCursor) | DONE | P0 |
| Memory budget per query | DONE | P0 |
| Spill to disk for large sorts | DONE | P1 |
| Adaptive strategy selection | DONE | P1 |
| Plan caching (wired into query handler) | DONE | P1 |
| CompactValue with SSO | DONE | P1 |

### 3.2 Cursor Implementations (124 of ~124 QuestDB strategies)

All 124 cursor types are implemented (see ARCHITECTURE.md for full list).

### 3.3 Query Optimizer

| Feature | Status | Priority |
|---------|--------|----------|
| Partition pruning | DONE | - |
| Index scan selection | DONE | - |
| Predicate pushdown | DONE | - |
| Limit pushdown | DONE | - |
| Table statistics | DONE | - |
| Cost-based JOIN ordering | Not implemented | P1 |
| Filter selectivity estimation | Not implemented | P1 |
| Common subexpression elimination | Not implemented | P2 |
| Constant folding | Not implemented | P2 |
| Query plan caching | DONE | P1 |

---

## Milestone 4: Storage Engine Hardening -- ~85% DONE

### 4.1 File Formats

| File | Status |
|------|--------|
| `.d` (data) | DONE |
| `.i` (index offsets) | DONE |
| `.k` (bitmap key) | DONE |
| `.v` (bitmap value) | DONE |
| `_meta` (metadata) | DONE (JSON + binary) |
| `_txn` (transactions) | DONE |
| `_cv` (column versions) | DONE |
| `_txn_scoreboard` | DONE (in-memory) |
| `data.parquet` (Apache) | DONE (writer: real PAR1, reader: partial) |
| `.checkpoint/` | DONE |

### 4.2 Storage Operations

| Operation | Status |
|-----------|--------|
| Compression (LZ4, delta, RLE, dictionary) | DONE |
| Out-of-order merge | DONE |
| Partition detach/attach | DONE |
| Partition squashing | DONE |
| Checksums per partition | DONE |
| Tiered storage (background jobs) | DONE |
| Snapshot + PITR (background jobs) | DONE |
| Vacuum / space reclaim | DONE |
| Encryption at rest (ChaCha20-Poly1305) | DONE |
| MVCC snapshot isolation (wired) | DONE |
| io_uring reader abstraction | DONE |
| Real Apache Parquet writer | DONE |
| Real Apache Parquet reader | Partial (P1) |

---

## Milestone 5: Network & Protocol Compliance -- ~80% DONE

### 5.1 PostgreSQL Wire Protocol

| Feature | Status |
|---------|--------|
| Simple Query Protocol | DONE |
| Extended Query Protocol | DONE |
| Prepared statements | DONE |
| COPY IN/OUT | DONE |
| Type OID mapping | DONE (core types) |
| Error code compliance (SQLSTATE) | Partial |
| pg_catalog | Partial (~7 tables) |
| information_schema | Partial (2 tables) |

### 5.2 ILP

| Feature | Status |
|---------|--------|
| TCP ingestion | DONE |
| HTTP ingestion | DONE |
| UDP ingestion | DONE |
| Authentication | DONE |
| TLS | DONE |

### 5.3 HTTP API

| Feature | Status |
|---------|--------|
| `/api/v1/query` | DONE |
| `/api/v1/write` | DONE |
| `/api/v1/health` | DONE |
| `/api/v1/tables` | DONE |
| `/api/v1/export` (CSV) | DONE |
| `/api/v1/import` (CSV) | DONE |
| `/metrics` (Prometheus) | DONE |
| WebSocket streaming | DONE |
| Web console UI | DONE |
| Rate limiting (wired as middleware) | DONE |
| CORS headers | DONE |
| Connection pooling (wired as middleware) | DONE |
| Session management (wired into handlers) | DONE |

---

## Milestone 6: Enterprise Features -- ~90% DONE

### 6.1 Replication

| Feature | Status |
|---------|--------|
| WAL shipping (primary -> replica) | DONE |
| Replica read-only mode | DONE |
| Failover promote/demote | DONE (manual) |
| Cluster management | DONE |
| Query routing | DONE |
| Automatic failover detection | Not done (P1) |
| Consensus protocol (Raft) | DONE (basic) |

### 6.2 Security

| Feature | Status |
|---------|--------|
| Token auth | DONE |
| OAuth 2.0 / OIDC | DONE |
| Service accounts | DONE |
| RBAC (table-level) | DONE + enforced |
| RBAC (column-level) | DONE (model) |
| Row-level security (wired into executor) | DONE |
| Encryption at rest (ChaCha20-Poly1305) | DONE |
| Audit log (wired into executor) | DONE |
| TLS | DONE |

### 6.3 Multi-tenancy

| Feature | Status |
|---------|--------|
| Namespace isolation | DONE (X-Tenant-ID routing) |
| Usage metering (wired) | DONE |
| Per-tenant resource quotas | Not done (P2) |

---

## Milestone 7: Observability & Operations -- ~95% DONE

| Feature | Status |
|---------|--------|
| Prometheus metrics (all counters wired) | DONE |
| Slow query log (wired into query handler) | DONE |
| Query profiling (EXPLAIN ANALYZE) | DONE |
| Log rotation | DONE |
| Hot config reload (SIGHUP) | DONE |
| Diagnostics endpoint | DONE |
| Health checks (real checks running) | DONE |
| Backup (snapshot) | DONE |
| Admin REST API | DONE |
| Plan cache (wired into query handler) | DONE |
| Resource management (wired into AppState) | DONE |

---

## Milestone 8: Testing & Quality -- ~70% DONE

| Feature | Status | Target |
|---------|--------|--------|
| Unit/integration tests (~26,500) | DONE | Expand coverage |
| SQL conformance tests | DONE | Expand |
| Crash recovery tests | DONE | Expand |
| Fuzz testing (ILP parser) | Partial | P1 |
| Fuzz testing (SQL parser) | Not done | P1 |
| Benchmark suite (TSBS) | Partial | P1 |
| Load testing | Not done | P1 |

---

## Milestone 9: Performance -- ~80% DONE

| Feature | Status |
|---------|--------|
| SIMD aggregations | DONE |
| SIMD string ops | DONE |
| Columnar execution path | DONE |
| Parallel partition scan | DONE |
| Parallel sort (k-way merge) | DONE |
| Parallel GROUP BY | DONE |
| Vectorized GROUP BY | DONE |
| Adaptive execution | DONE |
| Spill-to-disk | DONE |
| String interning | DONE |
| CompactValue (SSO) | DONE |
| io_uring reader (Linux) | DONE |
| JIT filter compilation (cranelift) | Not done (P2) |
| CPU affinity | Not done (P2) |

---

## Estimated Effort (Remaining)

| Milestone | Remaining Work | Estimate (1 dev) |
|-----------|---------------|------------------|
| M1: SQL Completeness | NATURAL JOIN, CALL | 1 week |
| M2: Functions (cast permutations, catalog) | Cast permutations, system catalog | 2-3 weeks |
| M3: Execution Engine | Cost-based optimizer, constant folding | 3-4 weeks |
| M4: Storage | Apache Parquet reader, persistent scoreboard | 2-3 weeks |
| M5: Network | pg_catalog completeness, HTTP/2 | 2-3 weeks |
| M6: Enterprise | Automatic failover, tenant quotas | 2-3 weeks |
| M7: Observability | Complete | - |
| M8: Testing | SQL fuzz tests, load tests | 2-3 weeks |
| M9: Performance | JIT, CPU affinity | 2-3 weeks |
| **Total remaining** | | **16-23 weeks** |

## Recommended Next Steps

1. **Apache Parquet reader** -- complete the read path for real PAR1 files
2. **Expand pg_catalog** -- needed for Grafana/DBeaver compatibility
3. **Cost-based optimizer** -- JOIN ordering, selectivity estimation
4. **Fuzz testing** -- SQL parser, ILP parser, WAL codec
5. **Automatic failover** -- consensus-based promotion
