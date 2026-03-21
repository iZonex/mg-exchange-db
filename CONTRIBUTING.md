# Contributing to ExchangeDB

Thank you for your interest in contributing! This guide will help you get started.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).
By participating, you agree to uphold this code.

## Getting Started

### Prerequisites

- Rust 1.85+ (install via [rustup](https://rustup.rs/))
- Docker (optional, for container builds)

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Check compilation without building
cargo check
```

The release binary will be at `target/release/exchange-db`.

### Running Tests

```bash
# Full test suite (~26,500 tests)
cargo test --workspace

# Specific crate
cargo test -p exchange-core
cargo test -p exchange-query
cargo test -p exchange-net

# Specific test
cargo test -p exchange-query test_sample_by

# Ignored tests (known issues)
cargo test -- --ignored
```

### Running Benchmarks

```bash
# All benchmarks
cargo bench

# Specific crate
cargo bench -p exchange-core
cargo bench -p exchange-query

# Specific benchmark
cargo bench -p exchange-core -- column_write
```

Results are tracked in [BENCHMARKS.md](BENCHMARKS.md).

## Development Workflow

1. Fork the repository
2. Create a feature branch from `main`: `git checkout -b feat/my-feature`
3. Make your changes
4. Run checks locally (see below)
5. Commit with a clear message
6. Push and open a Pull Request

### Branch Naming

| Prefix | Purpose |
|--------|---------|
| `feat/` | New feature |
| `fix/` | Bug fix |
| `perf/` | Performance improvement |
| `docs/` | Documentation only |
| `refactor/` | Code refactoring |
| `test/` | Adding or fixing tests |
| `ci/` | CI/CD changes |

### Pre-submit Checklist

Run these before pushing:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

## Code Style

- Run `cargo fmt` before committing
- Run `cargo clippy` and fix all warnings
- All public APIs must have doc comments
- Tests required for new features
- Prefer editing existing files over creating new ones
- No `unwrap()` in production code paths — use `?` or proper error handling

## Project Structure

```
crates/
  common/    Shared types, error handling, and utilities
  core/      Storage engine (columnar files, mmap, WAL, partitioning, SIMD)
  query/     SQL query engine (parser, planner, optimizer, executor)
  net/       Network protocols (PostgreSQL wire, HTTP REST, ILP ingestion)
  exchange/  Exchange-specific features (OHLCV, order books, tick encoding)
  server/    CLI and server binary
```

## Key Design Principles

- **Columnar storage**: data is stored column-by-column for cache-friendly scans
- **Partition-by-time**: tables are partitioned by timestamp for efficient time-range queries
- **Zero-copy reads**: column files are mmap'd for zero-copy access
- **SIMD aggregation**: numeric aggregates use SIMD intrinsics where available
- **Vectorized GROUP BY**: GROUP BY queries process columns directly, bypassing row materialization

## Pull Request Guidelines

- Keep PRs focused — one feature or fix per PR
- Write a clear description of **what** and **why**
- Link related issues with `Closes #123`
- Add tests for new functionality
- Update documentation if behavior changes
- All CI checks must pass

## Reporting Bugs

Use the [Bug Report](https://github.com/iZonex/mg-exchange-db/issues/new?template=bug_report.yml) template.

## Requesting Features

Use the [Feature Request](https://github.com/iZonex/mg-exchange-db/issues/new?template=feature_request.yml) template.

## Security Vulnerabilities

**Do NOT open a public issue for security vulnerabilities.**
See [SECURITY.md](SECURITY.md) for responsible disclosure instructions.

## Running the Server

```bash
# Default settings
cargo run --release -- server

# Custom data directory
EXCHANGEDB_DATA_DIR=/path/to/data cargo run --release -- server
```

Default ports:
- **9000** — HTTP REST API + Web Console
- **8812** — PostgreSQL wire protocol
- **9009** — InfluxDB Line Protocol (ILP) ingestion

## Docker

```bash
# Build
docker build -t exchangedb .

# Run
docker run -p 9000:9000 -p 8812:8812 -p 9009:9009 -v exchangedb-data:/data exchangedb
```

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
