# ExchangeDB Replication Guide

ExchangeDB supports WAL-based replication from a primary server to one or more
read replicas. This guide covers architecture, setup, failover, and monitoring.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Replication Modes](#replication-modes)
3. [Setting Up the Primary](#setting-up-the-primary)
4. [Setting Up a Replica](#setting-up-a-replica)
5. [Failover Procedure](#failover-procedure)
6. [S3 WAL Shipping](#s3-wal-shipping)
7. [Monitoring Replication](#monitoring-replication)
8. [Troubleshooting](#troubleshooting-replication)
9. [Limitations](#limitations)

---

## Architecture

ExchangeDB uses WAL-based (Write-Ahead Log) replication:

```
                    WAL Segments
  +---------+     ------------->     +----------+
  | Primary | -----> WAL Shipper --> | Replica  |
  +---------+                        +----------+
       |                                  |
  [Read/Write]                      [Read-Only]
       |                                  |
   Port 8812                          Port 8812
   Port 9000                          Port 9000
   Port 9009
```

### How It Works

1. **Primary** writes all data changes to its WAL (Write-Ahead Log).
2. **WAL Shipper** continuously sends completed WAL segments to replicas.
3. **Replica** receives WAL segments via the **WAL Receiver** and applies
   them to its local column store.
4. The replica serves read-only queries from its local data.
5. Write attempts on a replica are rejected with a `403 Forbidden` error.

### Components

| Component | Location | Description |
|-----------|----------|-------------|
| WalShipper | Primary | Sends WAL segments to replicas |
| WalReceiver | Replica | Receives and applies WAL segments |
| ReplicationManager | Both | Manages replication lifecycle |
| FailoverManager | Both | Handles promotion and demotion |

---

## Replication Modes

| Mode | Description |
|------|-------------|
| `standalone` | Default. No replication. |
| `primary` | Accepts writes, ships WAL to replicas. |
| `replica` | Read-only, receives WAL from primary. |

### Async vs Sync Replication

ExchangeDB currently supports **asynchronous replication**:

- The primary does not wait for replica acknowledgment before committing.
- There is a small window where a replica may be behind the primary.
- In the event of primary failure, some recently written data may not be
  available on the replica.

Synchronous replication (with quorum acknowledgment) is planned but not yet
fully validated.

---

## Setting Up the Primary

### 1. Configure the Primary

Create or edit `exchange-db.toml` on the primary server:

```toml
[server]
data_dir = "/data/exchangedb"
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

[storage]
wal_enabled = true
wal_max_segment_size = "64MB"

[replication]
mode = "primary"
```

### 2. Start the Primary

```bash
exchange-db server --config exchange-db.toml
```

### 3. Verify Primary Status

```bash
curl http://localhost:9000/admin/replication
```

```json
{
  "role": "primary",
  "lag_bytes": 0,
  "lag_seconds": 0,
  "segments_shipped": 0,
  "segments_applied": 0
}
```

---

## Setting Up a Replica

### 1. Take a Base Snapshot

Before starting a replica, create a consistent base snapshot from the primary:

```bash
# On the primary, flush WAL
curl -X POST http://primary:9000/admin/checkpoint

# Copy data directory to the replica
rsync -av primary:/data/exchangedb/ /data/exchangedb/
```

### 2. Configure the Replica

Create `exchange-db.toml` on the replica server:

```toml
[server]
data_dir = "/data/exchangedb"
log_level = "info"

[http]
bind = "0.0.0.0:9000"
enabled = true

[pgwire]
bind = "0.0.0.0:8812"
enabled = true

[ilp]
bind = "0.0.0.0:9009"
enabled = false  # Replicas don't accept ILP writes

[storage]
wal_enabled = true

[replication]
mode = "replica"
primary_addr = "primary-host:9009"  # Address of the primary server
```

### 3. Start the Replica

```bash
exchange-db server --config exchange-db.toml
```

### 4. Verify Replica Status

```bash
curl http://localhost:9000/admin/replication
```

```json
{
  "role": "replica",
  "lag_bytes": 1048576,
  "lag_seconds": 2,
  "segments_shipped": 0,
  "segments_applied": 42
}
```

### 5. Test Read Queries

```bash
psql -h replica-host -p 8812 -c "SELECT count(*) FROM trades;"
```

### 6. Verify Write Rejection

```bash
curl -X POST http://replica-host:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "INSERT INTO trades VALUES (now(), '\''BTC/USD'\'', 65000.0, 1.5, '\''buy'\'')"}'
```

Expected response:

```json
{
  "error": "this is a read-only replica",
  "code": 403
}
```

---

## Failover Procedure

### Manual Failover

When the primary becomes unavailable, promote a replica to primary:

### 1. Stop the Failed Primary

If the primary is still running but unhealthy, stop it:

```bash
systemctl stop exchangedb
```

### 2. Promote the Replica

Update the replica's configuration:

```toml
[replication]
mode = "primary"
# Remove primary_addr
```

Restart the replica:

```bash
systemctl restart exchangedb
```

Or use the admin API (if available):

```bash
curl -X POST http://replica-host:9000/admin/replication \
  -H 'Content-Type: application/json' \
  -d '{"action": "promote"}'
```

### 3. Verify Promotion

```bash
curl http://replica-host:9000/admin/replication
```

```json
{
  "role": "primary",
  "lag_bytes": 0,
  "lag_seconds": 0,
  "segments_shipped": 0,
  "segments_applied": 42
}
```

### 4. Redirect Clients

Update client configurations to point to the new primary:

- Update DNS records or load balancer targets.
- Update application connection strings.
- Update ILP ingestion endpoints.

### 5. Rebuild the Old Primary as a Replica

Once the old primary is repaired:

```bash
# Take a snapshot from the new primary
rsync -av new-primary:/data/exchangedb/ /data/exchangedb/

# Configure as replica
# exchange-db.toml:
# [replication]
# mode = "replica"
# primary_addr = "new-primary-host:9009"

systemctl start exchangedb
```

### Automatic Failover (Experimental)

ExchangeDB includes a FailoverManager with state machine logic for automatic
failover detection. This feature is not yet fully validated for production use.

The automatic failover flow:

1. Replicas monitor the primary via heartbeat.
2. If heartbeats are missed for the configured timeout, replicas detect failure.
3. The replica with the most recent WAL position promotes itself.
4. Other replicas redirect to the new primary.

---

## S3 WAL Shipping

ExchangeDB supports WAL shipping via S3 for environments where direct TCP
connections between primary and replica are not practical.

### How It Works

1. The primary's **S3WalShipper** uploads completed WAL segments to an S3
   bucket.
2. The replica polls the S3 bucket for new segments and downloads them.
3. Downloaded segments are applied to the replica's column store.

### Configuration

S3 WAL shipping configuration is specified in the replication section.
The S3 client uses standard AWS credential chain (environment variables,
IAM roles, credential files).

### Environment Variables for S3

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```

### S3 Bucket Layout

```
s3://exchangedb-wal/
  trades/
    wal-0/
      _events
      timestamp.d
      price.d
    wal-1/
      ...
```

### Considerations

- S3 WAL shipping adds latency compared to direct TCP replication.
- Suitable for cross-region replication or disaster recovery.
- S3 costs apply for storage and API calls.
- The mock S3 implementation is available for testing.

---

## Monitoring Replication

### Replication Status

```bash
# On primary
curl http://primary:9000/admin/replication

# On replica
curl http://replica:9000/admin/replication
```

### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `lag_bytes` | Bytes behind primary | > 100MB |
| `lag_seconds` | Seconds behind primary | > 60s |
| `segments_shipped` | WAL segments sent by primary | Monotonically increasing |
| `segments_applied` | WAL segments applied by replica | Should track shipped |

### Health Check with Replication

```bash
curl http://localhost:9000/api/v1/health
```

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_secs": 86400,
  "replication": {
    "role": "primary",
    "lag_bytes": 0,
    "connected_replicas": 2
  }
}
```

### Alerting

Set up alerts for:

- **Replication lag > 60 seconds**: Replica is falling behind.
- **Replica disconnected**: WAL shipping is failing.
- **Primary unreachable**: May need failover.

Example Prometheus alert:

```yaml
groups:
  - name: exchangedb
    rules:
      - alert: ReplicationLagHigh
        expr: exchangedb_replication_lag_seconds > 60
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "ExchangeDB replication lag is high"
```

---

## Troubleshooting Replication

### Replica Not Connecting

1. Verify the primary is running and the replication port is accessible:
   ```bash
   nc -zv primary-host 9009
   ```
2. Check firewall rules between primary and replica.
3. Verify `primary_addr` in the replica configuration is correct.
4. Check logs on both primary and replica for connection errors.

### Replication Lag Growing

1. Check if the primary has high write throughput exceeding the replica's
   apply capacity.
2. Verify network bandwidth between primary and replica.
3. Check disk I/O on the replica (WAL apply is I/O-bound).
4. Consider increasing the replica's `query_parallelism` for faster WAL apply.

### Replica Has Stale Data

1. Check `segments_applied` vs `segments_shipped`.
2. Verify the WAL merge job is running on the replica:
   ```bash
   curl http://replica:9000/admin/jobs
   ```
3. If the replica is too far behind, consider re-syncing from a fresh snapshot.

### Split-Brain Prevention

After a failover, ensure the old primary does not rejoin as a second primary:

1. Before restarting the old primary, change its mode to `replica` or
   `standalone`.
2. Never run two servers in `primary` mode simultaneously.

---

## Limitations

- **Asynchronous only**: Synchronous replication with quorum acknowledgment
  is not yet validated end-to-end.
- **No read queries on replica during apply**: WAL apply blocks reads
  momentarily (short pauses).
- **No automatic failover in production**: The FailoverManager exists but
  has not been validated for production use.
- **S3 WAL shipping not configurable via TOML**: Requires manual setup.
- **Single primary**: Only one primary is supported. Multi-primary (active-active)
  replication is not available.
- **No partial replication**: All tables are replicated. Table-level replication
  filtering is not supported.
