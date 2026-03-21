# --------------------------------------------------------------------------
# Builder stage
# --------------------------------------------------------------------------
FROM rust:1.87-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first for layer caching
COPY Cargo.toml Cargo.lock* ./
COPY crates/common/Cargo.toml crates/common/Cargo.toml
COPY crates/core/Cargo.toml crates/core/Cargo.toml
COPY crates/query/Cargo.toml crates/query/Cargo.toml
COPY crates/net/Cargo.toml crates/net/Cargo.toml
COPY crates/exchange/Cargo.toml crates/exchange/Cargo.toml
COPY crates/server/Cargo.toml crates/server/Cargo.toml

# Create dummy source files so cargo can resolve the workspace
RUN mkdir -p crates/common/src && echo "" > crates/common/src/lib.rs && \
    mkdir -p crates/core/src && echo "" > crates/core/src/lib.rs && \
    mkdir -p crates/query/src && echo "" > crates/query/src/lib.rs && \
    mkdir -p crates/net/src && echo "" > crates/net/src/lib.rs && \
    mkdir -p crates/exchange/src && echo "" > crates/exchange/src/lib.rs && \
    mkdir -p crates/server/src && echo "fn main() {}" > crates/server/src/main.rs

# Pre-build dependencies (cached unless Cargo.toml changes)
RUN cargo build --release 2>/dev/null || true

# Copy actual source code
COPY crates/ crates/

# Touch source files to invalidate the dummy build cache
RUN find crates -name "*.rs" -exec touch {} +

# Build the real binary
RUN cargo build --release --bin exchange-db

# --------------------------------------------------------------------------
# Runner stage
# --------------------------------------------------------------------------
FROM debian:bookworm-slim AS runner

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash exchangedb

COPY --from=builder /build/target/release/exchange-db /usr/local/bin/exchange-db

RUN mkdir -p /data && chown exchangedb:exchangedb /data

VOLUME ["/data"]

USER exchangedb
WORKDIR /data

# HTTP REST API
EXPOSE 9000
# PostgreSQL wire protocol
EXPOSE 8812
# InfluxDB Line Protocol
EXPOSE 9009

ENV EXCHANGEDB_DATA_DIR=/data

CMD ["exchange-db", "server"]
