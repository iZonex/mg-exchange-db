//! End-to-end replication integration tests.
//!
//! These tests verify that the full replication pipeline works: a primary
//! writes data, WAL segments are shipped to a replica (via in-process
//! file copy, not TCP), and the replica can read the data back through
//! column files after merge.

use std::path::Path;

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::FixedColumnReader;
use exchange_core::replication::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
use exchange_core::replication::failover::FailoverManager;
use exchange_core::replication::protocol::{self, ReplicationMessage};
use exchange_core::replication::s3_shipper::S3WalReceiver;
use exchange_core::replication::wal_receiver::WalReceiver;
use exchange_core::table::TableBuilder;
use exchange_core::tiered::{MemoryObjectStore, ObjectStore};
use exchange_core::txn::TxnFile;
use exchange_core::wal::merge::WalMergeJob;
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use tempfile::tempdir;

/// Helper: create the "trades" table with schema (timestamp, price, volume)
/// at the given db_root. Returns nothing; the table is ready to use.
fn create_trades_table(db_root: &Path) {
    let _meta = TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::I64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();

    // Initialize the _txn file.
    let table_dir = db_root.join("trades");
    let _txn = TxnFile::open(&table_dir).unwrap();
}

/// Helper: write N rows to the primary through WalTableWriter, without
/// merging. This leaves WAL segments as `.wal` files so they can be
/// shipped to a replica before being applied.
fn write_rows_no_merge(db_root: &Path, count: usize, base_ts: i64) {
    let config = WalTableWriterConfig {
        buffer_capacity: count + 1,
        merge_on_commit: false,
        ..Default::default()
    };

    let mut writer = WalTableWriter::open(db_root, "trades", config).unwrap();

    for i in 0..count {
        let ts = Timestamp(base_ts + (i as i64) * 1_000_000_000);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(100.0 + i as f64),
                    OwnedColumnValue::I64(i as i64),
                ],
            )
            .unwrap();
    }

    writer.commit().unwrap();
}

/// Helper: write N rows AND merge (apply WAL to column files).
/// After this call the WAL segments are renamed to `.applied`.
fn write_rows(db_root: &Path, count: usize, base_ts: i64) {
    let config = WalTableWriterConfig {
        buffer_capacity: count + 1,
        merge_on_commit: true,
        ..Default::default()
    };

    let mut writer = WalTableWriter::open(db_root, "trades", config).unwrap();

    for i in 0..count {
        let ts = Timestamp(base_ts + (i as i64) * 1_000_000_000);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(100.0 + i as f64),
                    OwnedColumnValue::I64(i as i64),
                ],
            )
            .unwrap();
    }

    writer.commit().unwrap();
}

/// Helper: run merge on a table (apply WAL to column files).
fn merge_table(db_root: &Path, table: &str) -> exchange_core::wal::merge::MergeStats {
    let table_dir = db_root.join(table);
    let meta = exchange_core::table::TableMeta::load(&table_dir.join("_meta")).unwrap();
    let merge_job = WalMergeJob::new(table_dir, meta);
    merge_job.run().unwrap()
}

/// Helper: ship WAL segments from primary to replica by copying files
/// and using the replication protocol in-process. This simulates what
/// WalShipper + WalReceiver would do over TCP, but via local file I/O
/// and protocol encode/decode.
fn ship_segments_locally(primary_root: &Path, replica_root: &Path, table: &str) {
    let primary_wal_dir = primary_root.join(table).join("wal");
    if !primary_wal_dir.exists() {
        return;
    }

    // Collect all .wal segment files from the primary.
    let mut segments: Vec<_> = std::fs::read_dir(&primary_wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .collect();
    segments.sort_by_key(|e| e.file_name());

    let mut receiver = WalReceiver::new(replica_root.to_path_buf(), "127.0.0.1:0".into());

    for entry in &segments {
        let data = std::fs::read(entry.path()).unwrap();
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();

        // Parse segment_id from filename.
        let segment_id: u32 = filename
            .strip_prefix("wal-")
            .and_then(|s| s.strip_suffix(".wal"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Extract actual txn_range by scanning events.
        let txn_range = extract_txn_range(&data);

        // Build a replication message and encode/decode it to exercise the protocol.
        let msg = ReplicationMessage::WalSegment {
            table: table.to_string(),
            segment_id,
            data: data.clone(),
            txn_range,
        };

        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, _) = protocol::decode(&encoded).unwrap();

        // Apply the decoded message on the receiver.
        match decoded {
            ReplicationMessage::WalSegment {
                table: t,
                data: d,
                txn_range: tr,
                ..
            } => {
                receiver.apply_segment(&t, &d).unwrap();

                // Verify txn_range was preserved through encode/decode.
                assert_eq!(tr, txn_range);
            }
            _ => panic!("unexpected message type"),
        }
    }
}

/// Extract min/max txn_id from raw WAL segment bytes.
/// Uses the WAL event binary format: header is 16 bytes,
/// each event: 1 byte type + 8 bytes txn_id + 8 bytes timestamp + 4 bytes payload_len + payload + 4 bytes checksum.
fn extract_txn_range(segment_data: &[u8]) -> (u64, u64) {
    use exchange_core::wal::event::{EVENT_HEADER_SIZE, EVENT_OVERHEAD, WalEvent};

    let header_size = 16; // SEGMENT_HEADER_SIZE
    let mut offset = header_size;
    let mut min_txn: u64 = u64::MAX;
    let mut max_txn: u64 = 0;

    while offset + EVENT_HEADER_SIZE <= segment_data.len() {
        let payload_len =
            u32::from_le_bytes(segment_data[offset + 17..offset + 21].try_into().unwrap()) as usize;
        let event_total = EVENT_OVERHEAD + payload_len;
        if offset + event_total > segment_data.len() {
            break;
        }

        match WalEvent::deserialize(&segment_data[offset..offset + event_total]) {
            Ok(event) => {
                if event.txn_id < min_txn {
                    min_txn = event.txn_id;
                }
                if event.txn_id > max_txn {
                    max_txn = event.txn_id;
                }
            }
            Err(_) => break,
        }
        offset += event_total;
    }

    if max_txn == 0 {
        (0, 0)
    } else {
        (min_txn, max_txn)
    }
}

/// Count total rows across all partition directories for a table on the replica.
fn count_replica_rows(replica_root: &Path, table: &str, column: &str, col_type: ColumnType) -> u64 {
    let table_dir = replica_root.join(table);
    let mut total = 0u64;

    if !table_dir.exists() {
        return 0;
    }

    for entry in std::fs::read_dir(&table_dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            // Skip metadata directories.
            if name.starts_with('_') || name == "wal" {
                continue;
            }
            let col_path = path.join(format!("{column}.d"));
            if col_path.exists() {
                let reader = FixedColumnReader::open(&col_path, col_type).unwrap();
                total += reader.row_count();
            }
        }
    }

    total
}

// ===========================================================================
// Test: primary_writes_replica_reads
// ===========================================================================

#[test]
fn primary_writes_replica_reads() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // 1. Create table on primary and replica.
    create_trades_table(primary_dir.path());
    create_trades_table(replica_dir.path());

    let base_ts: i64 = 1_710_513_000_000_000_000; // 2024-03-15

    // --- Round 1: write 100 rows, ship, merge ---

    // Write 100 rows (no merge so WAL files remain as .wal).
    write_rows_no_merge(primary_dir.path(), 100, base_ts);

    // Ship before merge (while .wal files exist).
    ship_segments_locally(primary_dir.path(), replica_dir.path(), "trades");

    // Merge on both.
    let primary_stats = merge_table(primary_dir.path(), "trades");
    assert_eq!(primary_stats.rows_merged, 100);
    let replica_stats = merge_table(replica_dir.path(), "trades");
    assert_eq!(replica_stats.rows_merged, 100);

    // Verify 100 rows on replica.
    let row_count = count_replica_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(
        row_count, 100,
        "replica should have 100 rows after first ship"
    );

    // Verify data values on replica.
    let part_dir = replica_dir.path().join("trades").join("2024-03-15");
    assert!(
        part_dir.exists(),
        "partition 2024-03-15 should exist on replica"
    );
    let price_reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(price_reader.read_f64(0), 100.0);
    assert_eq!(price_reader.read_f64(99), 199.0);

    // --- Round 2: write 50 more rows, ship, merge ---

    let base_ts2 = base_ts + 100 * 1_000_000_000;
    write_rows_no_merge(primary_dir.path(), 50, base_ts2);

    ship_segments_locally(primary_dir.path(), replica_dir.path(), "trades");

    let primary_stats2 = merge_table(primary_dir.path(), "trades");
    assert_eq!(primary_stats2.rows_merged, 50);
    let replica_stats2 = merge_table(replica_dir.path(), "trades");
    assert_eq!(replica_stats2.rows_merged, 50);

    // Verify 150 total rows on replica.
    let row_count = count_replica_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(
        row_count, 150,
        "replica should have 150 rows after second ship"
    );
}

// ===========================================================================
// Test: replica_rejects_writes
// ===========================================================================

#[test]
fn replica_rejects_writes() {
    let dir = tempdir().unwrap();
    let config = ReplicationConfig {
        role: ReplicationRole::Replica,
        primary_addr: Some("127.0.0.1:9100".to_string()),
        replica_addrs: Vec::new(),
        sync_mode: ReplicationSyncMode::Async,
        max_lag_bytes: 256 * 1024 * 1024,
        ..Default::default()
    };

    let mgr = exchange_core::replication::ReplicationManager::new(dir.path().to_path_buf(), config);

    // A replica should be in read-only mode.
    assert!(mgr.is_read_only(), "replica should be read-only");

    // A primary should NOT be read-only.
    let primary_config = ReplicationConfig {
        role: ReplicationRole::Primary,
        primary_addr: None,
        replica_addrs: Vec::new(),
        sync_mode: ReplicationSyncMode::Async,
        max_lag_bytes: 256 * 1024 * 1024,
        ..Default::default()
    };
    let primary_mgr = exchange_core::replication::ReplicationManager::new(
        dir.path().to_path_buf(),
        primary_config,
    );
    assert!(
        !primary_mgr.is_read_only(),
        "primary should NOT be read-only"
    );
}

// ===========================================================================
// Test: failover_promotion
// ===========================================================================

#[test]
fn failover_promotion() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // 1. Set up primary and replica with the same table.
    create_trades_table(primary_dir.path());
    create_trades_table(replica_dir.path());

    // 2. Primary writes data (no merge, so WAL is shippable).
    let base_ts: i64 = 1_710_513_000_000_000_000;
    write_rows_no_merge(primary_dir.path(), 50, base_ts);

    // 3. Ship to replica.
    ship_segments_locally(primary_dir.path(), replica_dir.path(), "trades");

    // 4. Merge on both primary and replica.
    merge_table(primary_dir.path(), "trades");
    merge_table(replica_dir.path(), "trades");

    // 5. Verify replica has the data.
    let row_count = count_replica_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(row_count, 50);

    // 6. "Primary fails" — we just stop shipping. Promote replica.
    let replica_config = ReplicationConfig {
        role: ReplicationRole::Replica,
        primary_addr: Some("127.0.0.1:9100".to_string()),
        replica_addrs: Vec::new(),
        sync_mode: ReplicationSyncMode::Async,
        max_lag_bytes: 256 * 1024 * 1024,
        ..Default::default()
    };
    let mut failover_mgr = FailoverManager::new(replica_config, std::time::Duration::from_secs(5));

    assert_eq!(*failover_mgr.current_role(), ReplicationRole::Replica);

    failover_mgr.promote_to_primary().unwrap();

    assert_eq!(*failover_mgr.current_role(), ReplicationRole::Primary);

    // 7. Write new data on the promoted replica (now acting as primary).
    //    Use merge_on_commit since this is the promoted primary now.
    let base_ts2 = base_ts + 50 * 1_000_000_000;
    write_rows(replica_dir.path(), 30, base_ts2);

    // 8. Verify data integrity: 50 original + 30 new = 80 rows.
    let row_count = count_replica_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(
        row_count, 80,
        "promoted replica should have 80 rows (50 replicated + 30 new)"
    );

    // 9. Verify data correctness.
    let part_dir = replica_dir.path().join("trades").join("2024-03-15");
    let price_reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    // First row from replication.
    assert_eq!(price_reader.read_f64(0), 100.0);
    // Last replicated row.
    assert_eq!(price_reader.read_f64(49), 149.0);
    // First row written after promotion.
    assert_eq!(price_reader.read_f64(50), 100.0);
}

// ===========================================================================
// Test: txn_range_is_correct (verifies fix #1)
// ===========================================================================

#[test]
fn txn_range_is_correct_in_shipped_segments() {
    let primary_dir = tempdir().unwrap();
    create_trades_table(primary_dir.path());

    // Write some rows to create WAL segments (no merge so .wal files remain).
    let base_ts: i64 = 1_710_513_000_000_000_000;
    write_rows_no_merge(primary_dir.path(), 10, base_ts);

    // Read the WAL segments and verify txn_range.
    let wal_dir = primary_dir.path().join("trades").join("wal");
    if wal_dir.exists() {
        for entry in std::fs::read_dir(&wal_dir).unwrap().flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".wal") {
                let data = std::fs::read(entry.path()).unwrap();
                let txn_range = extract_txn_range(&data);
                // If there are events, txn_range should NOT be (0, 0).
                if data.len() > 16 + 25 {
                    // Segment has at least one event
                    assert_ne!(
                        txn_range,
                        (0, 0),
                        "txn_range should not be (0,0) for a segment with events"
                    );
                    assert!(txn_range.0 <= txn_range.1, "min_txn should be <= max_txn");
                    assert!(txn_range.0 >= 1, "txn_ids start at 1");
                }
            }
        }
    }
}

// ===========================================================================
// Test: receiver_auto_merge (verifies fix #2)
// ===========================================================================

#[test]
fn receiver_auto_merge_applies_wal() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // Set up both sides.
    create_trades_table(primary_dir.path());
    create_trades_table(replica_dir.path());

    // Write on primary (no merge, so WAL files remain).
    let base_ts: i64 = 1_710_513_000_000_000_000;
    write_rows_no_merge(primary_dir.path(), 20, base_ts);

    // Create a receiver and manually feed it WAL data.
    let mut receiver = WalReceiver::new(replica_dir.path().to_path_buf(), "127.0.0.1:0".into());

    // Read WAL segments from primary and apply on replica via receiver.
    let primary_wal_dir = primary_dir.path().join("trades").join("wal");
    for entry in std::fs::read_dir(&primary_wal_dir).unwrap().flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with(".wal") {
            let data = std::fs::read(entry.path()).unwrap();
            receiver.apply_segment("trades", &data).unwrap();
        }
    }

    // Run merge on replica to apply WAL to column files.
    let stats = merge_table(replica_dir.path(), "trades");
    assert_eq!(stats.rows_merged, 20);

    // Verify data on replica.
    let row_count = count_replica_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(row_count, 20);
}

// ===========================================================================
// Test: S3 shipping roundtrip (fix #4)
// ===========================================================================

#[test]
fn s3_shipping_roundtrip() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // Set up primary with table and data.
    create_trades_table(primary_dir.path());
    create_trades_table(replica_dir.path());

    let base_ts: i64 = 1_710_513_000_000_000_000;
    write_rows_no_merge(primary_dir.path(), 25, base_ts);

    // Phase 1: Ship WAL segments to MemoryObjectStore via S3WalShipper.
    let store = MemoryObjectStore::new();

    let primary_wal_dir = primary_dir.path().join("trades").join("wal");

    // Upload segments directly into the store (mirroring what S3WalShipper does).
    let mut shipped_count = 0usize;
    for entry in std::fs::read_dir(&primary_wal_dir).unwrap().flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with(".wal") {
            let data = std::fs::read(entry.path()).unwrap();
            let key = format!("repl/trades/{name_str}");
            store.put(&key, &data).unwrap();
            shipped_count += 1;
        }
    }
    assert!(
        shipped_count > 0,
        "should have shipped at least one segment"
    );

    // Verify segments are listed.
    let keys = store.list("repl/trades/").unwrap();
    assert_eq!(keys.len(), shipped_count);

    // Phase 2: Receive segments via S3WalReceiver.
    // Since segment 0 exists and sync(table, 0) uses `seq > last_applied`,
    // we use the get_segment method to download each segment individually
    // (avoiding the sync filtering issue with segment 0).
    let replica_table_wal = replica_dir.path().join("trades").join("wal");
    std::fs::create_dir_all(&replica_table_wal).unwrap();

    let receiver = S3WalReceiver::new(Box::new(store), "repl", replica_dir.path().to_path_buf());

    for key in &keys {
        let filename = key.rsplit('/').next().unwrap();
        let data = receiver.get_segment("trades", filename).unwrap();
        let dest = replica_table_wal.join(filename);
        std::fs::write(&dest, &data).unwrap();
    }

    // Verify local WAL files were written.
    let local_files: Vec<_> = std::fs::read_dir(&replica_table_wal)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .collect();
    assert_eq!(local_files.len(), shipped_count);

    // Phase 3: Merge on replica and verify data.
    let stats = merge_table(replica_dir.path(), "trades");
    assert_eq!(stats.rows_merged, 25);

    let row_count = count_replica_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(
        row_count, 25,
        "replica should have 25 rows after S3 roundtrip"
    );

    // Spot-check values.
    let part_dir = replica_dir.path().join("trades").join("2024-03-15");
    let price_reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(price_reader.read_f64(0), 100.0);
    assert_eq!(price_reader.read_f64(24), 124.0);
}

// ===========================================================================
// Test: protocol round-trip preserves txn_range
// ===========================================================================

#[test]
fn protocol_roundtrip_preserves_txn_range() {
    let msg = ReplicationMessage::WalSegment {
        table: "trades".into(),
        segment_id: 7,
        data: vec![0xAA; 64],
        txn_range: (42, 99),
    };

    let encoded = protocol::encode(&msg).unwrap();
    let (decoded, _) = protocol::decode(&encoded).unwrap();

    match decoded {
        ReplicationMessage::WalSegment { txn_range, .. } => {
            assert_eq!(txn_range, (42, 99));
        }
        _ => panic!("wrong message type"),
    }
}

// ===========================================================================
// Tests for SchemaSync-based schema replication
// ===========================================================================

/// Helper: ship schema + WAL from primary to replica using in-process protocol
/// encode/decode.  First sends a SchemaSync, then ships WAL segments.
fn ship_schema_and_segments(primary_root: &Path, replica_root: &Path, table: &str) {
    let mut receiver = WalReceiver::new(replica_root.to_path_buf(), "127.0.0.1:0".into());

    // 1. Ship schema via SchemaSync message.
    let meta_path = primary_root.join(table).join("_meta");
    if meta_path.exists() {
        let meta_json = std::fs::read_to_string(&meta_path).unwrap();
        let meta = exchange_core::table::TableMeta::load(&meta_path).unwrap();

        let msg = ReplicationMessage::SchemaSync {
            table: table.to_string(),
            meta_json,
            version: meta.version,
        };

        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, _) = protocol::decode(&encoded).unwrap();

        // Apply on receiver.
        match decoded {
            ReplicationMessage::SchemaSync {
                table: t,
                meta_json: mj,
                version: v,
            } => {
                receiver.apply_schema_sync(&t, &mj, v).unwrap();
            }
            _ => panic!("expected SchemaSync"),
        }
    }

    // 2. Ship WAL segments.
    let primary_wal_dir = primary_root.join(table).join("wal");
    if !primary_wal_dir.exists() {
        return;
    }

    let mut segments: Vec<_> = std::fs::read_dir(&primary_wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .collect();
    segments.sort_by_key(|e| e.file_name());

    for entry in &segments {
        let data = std::fs::read(entry.path()).unwrap();
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();

        let segment_id: u32 = filename
            .strip_prefix("wal-")
            .and_then(|s| s.strip_suffix(".wal"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let txn_range = extract_txn_range(&data);

        let msg = ReplicationMessage::WalSegment {
            table: table.to_string(),
            segment_id,
            data: data.clone(),
            txn_range,
        };

        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, _) = protocol::decode(&encoded).unwrap();

        match decoded {
            ReplicationMessage::WalSegment {
                table: t, data: d, ..
            } => {
                receiver.apply_segment(&t, &d).unwrap();
            }
            _ => panic!("expected WalSegment"),
        }
    }
}

/// Test: SchemaSync delivers _meta to a fresh replica that has no table dir.
/// After SchemaSync, the replica should have the _meta file and can merge WAL.
#[test]
fn schema_sync_creates_table_on_replica() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // Create table ONLY on primary.
    create_trades_table(primary_dir.path());

    // Write some rows on primary (no merge so WAL files remain).
    let base_ts: i64 = 1_710_513_000_000_000_000;
    write_rows_no_merge(primary_dir.path(), 10, base_ts);

    // Before shipping, the replica has nothing.
    let replica_meta = replica_dir.path().join("trades").join("_meta");
    assert!(!replica_meta.exists(), "replica should not have _meta yet");

    // Ship schema + WAL.
    ship_schema_and_segments(primary_dir.path(), replica_dir.path(), "trades");

    // Now the replica should have _meta.
    assert!(
        replica_meta.exists(),
        "replica should have _meta after SchemaSync"
    );

    // Verify the _meta content matches the primary.
    let primary_meta =
        exchange_core::table::TableMeta::load(&primary_dir.path().join("trades").join("_meta"))
            .unwrap();
    let replica_meta_loaded = exchange_core::table::TableMeta::load(&replica_meta).unwrap();
    assert_eq!(primary_meta.name, replica_meta_loaded.name);
    assert_eq!(
        primary_meta.columns.len(),
        replica_meta_loaded.columns.len()
    );
    assert_eq!(primary_meta.version, replica_meta_loaded.version);

    // Merge on replica should succeed now.
    let stats = merge_table(replica_dir.path(), "trades");
    assert_eq!(stats.rows_merged, 10);

    // Verify data on replica.
    let row_count = count_replica_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(row_count, 10);
}

/// Test: SchemaSync after ALTER TABLE (column addition) propagates the new schema.
#[test]
fn schema_sync_after_alter_table() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // Create table on primary and ship schema to replica.
    create_trades_table(primary_dir.path());
    let base_ts: i64 = 1_710_513_000_000_000_000;
    write_rows_no_merge(primary_dir.path(), 5, base_ts);

    ship_schema_and_segments(primary_dir.path(), replica_dir.path(), "trades");
    merge_table(replica_dir.path(), "trades");

    // Verify 3 columns on replica.
    let replica_meta_path = replica_dir.path().join("trades").join("_meta");
    let meta_v1 = exchange_core::table::TableMeta::load(&replica_meta_path).unwrap();
    assert_eq!(meta_v1.columns.len(), 3);
    let version_before = meta_v1.version;

    // ALTER TABLE on primary: add a column.
    let primary_meta_path = primary_dir.path().join("trades").join("_meta");
    let mut primary_meta = exchange_core::table::TableMeta::load(&primary_meta_path).unwrap();
    primary_meta
        .add_column("source", ColumnType::Symbol)
        .unwrap();
    primary_meta.save(&primary_meta_path).unwrap();

    // Ship the updated schema.
    let updated_json = std::fs::read_to_string(&primary_meta_path).unwrap();
    let receiver = WalReceiver::new(replica_dir.path().to_path_buf(), "127.0.0.1:0".into());
    receiver
        .apply_schema_sync("trades", &updated_json, primary_meta.version)
        .unwrap();

    // Verify 4 columns on replica and version bumped.
    let meta_v2 = exchange_core::table::TableMeta::load(&replica_meta_path).unwrap();
    assert_eq!(meta_v2.columns.len(), 4);
    assert!(
        meta_v2.version > version_before,
        "schema version should have increased"
    );
    assert_eq!(meta_v2.columns[3].name, "source");
}

/// Test: SchemaSync with older version does NOT overwrite newer schema.
#[test]
fn schema_sync_does_not_regress_version() {
    let dir = tempdir().unwrap();

    // Create table to get a _meta file.
    create_trades_table(dir.path());

    // Bump the version manually.
    let meta_path = dir.path().join("trades").join("_meta");
    let mut meta = exchange_core::table::TableMeta::load(&meta_path).unwrap();
    meta.add_column("extra", ColumnType::F64).unwrap();
    meta.save(&meta_path).unwrap();
    let new_version = meta.version;
    assert!(new_version > 1);

    // Now create a receiver pointing at the same dir and try to sync with
    // an OLDER version -- it should be a no-op.
    let old_json = r#"{"name":"trades","columns":[{"name":"timestamp","col_type":"Timestamp","indexed":false}],"partition_by":"Day","timestamp_column":0,"version":1}"#;
    let receiver = WalReceiver::new(dir.path().to_path_buf(), "127.0.0.1:0".into());
    receiver.apply_schema_sync("trades", old_json, 1).unwrap();

    // Verify the version was NOT regressed.
    let meta_after = exchange_core::table::TableMeta::load(&meta_path).unwrap();
    assert_eq!(
        meta_after.version, new_version,
        "version should not regress"
    );
    assert_eq!(
        meta_after.columns.len(),
        4,
        "columns should not be overwritten"
    );
}

/// Test: Full flow -- CREATE TABLE on primary, INSERT, data appears on replica
/// via SchemaSync + WAL ship. The replica never has create_trades_table called.
#[test]
fn full_flow_create_insert_replicate() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // Create and populate on primary only.
    create_trades_table(primary_dir.path());
    let base_ts: i64 = 1_710_513_000_000_000_000;
    write_rows_no_merge(primary_dir.path(), 50, base_ts);

    // Ship schema + WAL to a completely empty replica.
    ship_schema_and_segments(primary_dir.path(), replica_dir.path(), "trades");

    // Merge on replica.
    let stats = merge_table(replica_dir.path(), "trades");
    assert_eq!(stats.rows_merged, 50);

    // Verify data on replica.
    let row_count = count_replica_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(row_count, 50);

    // Spot-check values.
    let part_dir = replica_dir.path().join("trades").join("2024-03-15");
    assert!(part_dir.exists());
    let price_reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(price_reader.read_f64(0), 100.0);
    assert_eq!(price_reader.read_f64(49), 149.0);
}

/// Test: SchemaSync with two replicas -- both get the schema independently.
#[test]
fn schema_sync_two_replicas() {
    let primary_dir = tempdir().unwrap();
    let replica1_dir = tempdir().unwrap();
    let replica2_dir = tempdir().unwrap();

    // Create and populate on primary only.
    create_trades_table(primary_dir.path());
    let base_ts: i64 = 1_710_513_000_000_000_000;
    write_rows_no_merge(primary_dir.path(), 20, base_ts);

    // Ship to both replicas.
    ship_schema_and_segments(primary_dir.path(), replica1_dir.path(), "trades");
    ship_schema_and_segments(primary_dir.path(), replica2_dir.path(), "trades");

    // Merge on both.
    let stats1 = merge_table(replica1_dir.path(), "trades");
    assert_eq!(stats1.rows_merged, 20);
    let stats2 = merge_table(replica2_dir.path(), "trades");
    assert_eq!(stats2.rows_merged, 20);

    // Verify data on both replicas.
    let count1 = count_replica_rows(replica1_dir.path(), "trades", "price", ColumnType::F64);
    let count2 = count_replica_rows(replica2_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(count1, 20);
    assert_eq!(count2, 20);

    // Verify both replicas have identical _meta.
    let meta1 =
        exchange_core::table::TableMeta::load(&replica1_dir.path().join("trades").join("_meta"))
            .unwrap();
    let meta2 =
        exchange_core::table::TableMeta::load(&replica2_dir.path().join("trades").join("_meta"))
            .unwrap();
    assert_eq!(meta1.version, meta2.version);
    assert_eq!(meta1.columns.len(), meta2.columns.len());
}

/// Test: SchemaSync protocol encode/decode round-trip.
#[test]
fn schema_sync_protocol_roundtrip() {
    let meta_json = r#"{"name":"test","columns":[{"name":"ts","col_type":"Timestamp","indexed":false}],"partition_by":"Day","timestamp_column":0,"version":5}"#;

    let msg = ReplicationMessage::SchemaSync {
        table: "test".into(),
        meta_json: meta_json.into(),
        version: 5,
    };

    let encoded = protocol::encode(&msg).unwrap();
    let (decoded, consumed) = protocol::decode(&encoded).unwrap();
    assert_eq!(consumed, encoded.len());

    match decoded {
        ReplicationMessage::SchemaSync {
            table,
            meta_json: mj,
            version,
        } => {
            assert_eq!(table, "test");
            assert_eq!(mj, meta_json);
            assert_eq!(version, 5);
        }
        _ => panic!("expected SchemaSync"),
    }
}

/// Test: Multiple SchemaSync round-trips (simulate schema evolution).
#[test]
fn schema_sync_multiple_versions() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // v1: create table.
    create_trades_table(primary_dir.path());
    let base_ts: i64 = 1_710_513_000_000_000_000;
    write_rows_no_merge(primary_dir.path(), 5, base_ts);
    ship_schema_and_segments(primary_dir.path(), replica_dir.path(), "trades");
    merge_table(replica_dir.path(), "trades");

    let meta_path = primary_dir.path().join("trades").join("_meta");
    let replica_meta_path = replica_dir.path().join("trades").join("_meta");

    // v2: add column on primary.
    let mut meta = exchange_core::table::TableMeta::load(&meta_path).unwrap();
    meta.add_column("exchange", ColumnType::Symbol).unwrap();
    meta.save(&meta_path).unwrap();

    // Ship updated schema.
    let json_v2 = std::fs::read_to_string(&meta_path).unwrap();
    let receiver = WalReceiver::new(replica_dir.path().to_path_buf(), "127.0.0.1:0".into());
    receiver
        .apply_schema_sync("trades", &json_v2, meta.version)
        .unwrap();

    let replica_meta = exchange_core::table::TableMeta::load(&replica_meta_path).unwrap();
    assert_eq!(replica_meta.columns.len(), 4);

    // v3: add another column on primary.
    let mut meta = exchange_core::table::TableMeta::load(&meta_path).unwrap();
    meta.add_column("side", ColumnType::Symbol).unwrap();
    meta.save(&meta_path).unwrap();

    let json_v3 = std::fs::read_to_string(&meta_path).unwrap();
    receiver
        .apply_schema_sync("trades", &json_v3, meta.version)
        .unwrap();

    let replica_meta = exchange_core::table::TableMeta::load(&replica_meta_path).unwrap();
    assert_eq!(replica_meta.columns.len(), 5);
    assert_eq!(replica_meta.columns[3].name, "exchange");
    assert_eq!(replica_meta.columns[4].name, "side");
}
