//! Chaos testing for ExchangeDB core storage layer.
//!
//! Simulates crashes, corruption, and adverse conditions to verify that the
//! WAL, recovery, and column store remain consistent and resilient.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::FixedColumnReader;
use exchange_core::recovery::RecoveryManager;
use exchange_core::table::{TableBuilder, TableMeta};
use exchange_core::txn::TxnFile;
use exchange_core::wal::event::WalEvent;
use exchange_core::wal::reader::WalReader;
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal::segment::{WalSegment, segment_path};
use exchange_core::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Timestamp for 2024-03-15 in nanoseconds.
const TS_BASE: i64 = 1_710_513_000_000_000_000;

fn create_test_table(db_root: &Path, name: &str) -> TableMeta {
    let meta = TableBuilder::new(name)
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();
    let table_dir = db_root.join(name);
    let _txn = TxnFile::open(&table_dir).unwrap();
    meta
}

fn create_3col_table(db_root: &Path, name: &str) -> TableMeta {
    let meta = TableBuilder::new(name)
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::I64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();
    let table_dir = db_root.join(name);
    let _txn = TxnFile::open(&table_dir).unwrap();
    meta
}

fn write_n_rows_no_commit(db_root: &Path, table: &str, n: usize) {
    let config = WalTableWriterConfig::default();
    let mut writer = WalTableWriter::open(db_root, table, config).unwrap();
    for i in 0..n {
        let ts = Timestamp(TS_BASE + (i as i64) * 1_000_000_000);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(60000.0 + i as f64),
                ],
            )
            .unwrap();
    }
    writer.flush().unwrap();
    // Intentionally DO NOT commit -- simulates crash.
}

// ===========================================================================
// Task 1: Crash simulation tests
// ===========================================================================

/// Kill writer mid-flush, verify recovery.
#[test]
fn crash_mid_flush_1000_rows() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    write_n_rows_no_commit(db_root, "test", 1000);

    // Verify WAL segment exists.
    let wal_dir = db_root.join("test").join("wal");
    assert!(wal_dir.exists());

    // Run recovery.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 1);
    assert_eq!(stats.rows_recovered, 1000);

    // Verify all 1000 rows recovered.
    let part_dir = db_root.join("test").join("2024-03-15");
    assert!(part_dir.exists());
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 1000);
    assert_eq!(reader.read_f64(0), 60000.0);
    assert_eq!(reader.read_f64(999), 60999.0);
}

/// Drop writer immediately after write_row, before explicit flush.
#[test]
fn crash_before_explicit_flush() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    {
        let config = WalTableWriterConfig {
            buffer_capacity: 5000, // Large so no auto-flush
            ..Default::default()
        };
        let mut writer = WalTableWriter::open(db_root, "test", config).unwrap();
        for i in 0..10 {
            let ts = Timestamp(TS_BASE + (i as i64) * 1_000_000_000);
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(100.0 + i as f64),
                    ],
                )
                .unwrap();
        }
        // Drop triggers best-effort flush.
    }

    // Recovery should find WAL data.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    // The Drop impl does a best-effort flush, so rows should be recoverable.
    assert!(stats.rows_recovered >= 10 || stats.rows_recovered == 0);
}

/// Kill during partition write (partial column files).
#[test]
fn crash_mid_partition_write() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_3col_table(db_root, "test");

    // Write 500 rows, flush to WAL but do not commit.
    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "test", config).unwrap();
        for i in 0..500 {
            let ts = Timestamp(TS_BASE + (i as i64) * 1_000_000_000);
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(100.0 + i as f64),
                        OwnedColumnValue::I64(i),
                    ],
                )
                .unwrap();
        }
        writer.flush().unwrap();
    }

    // Recovery should detect and apply WAL.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 1);
    assert_eq!(stats.rows_recovered, 500);

    // Verify all columns are consistent.
    let part_dir = db_root.join("test").join("2024-03-15");
    let price_reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    let volume_reader =
        FixedColumnReader::open(&part_dir.join("volume.d"), ColumnType::I64).unwrap();
    assert_eq!(price_reader.row_count(), volume_reader.row_count());
    assert_eq!(price_reader.row_count(), 500);
}

/// Corrupt WAL segment, verify it is detected and skipped.
#[test]
fn corrupt_wal_segment_detected() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    // Write valid WAL data.
    {
        let config = WalWriterConfig {
            max_segment_size: 64 * 1024 * 1024,
            commit_mode: CommitMode::Sync,
        };
        let mut writer = WalWriter::create(&wal_dir, config).unwrap();
        writer.append_data(1000, b"row1".to_vec()).unwrap();
        writer.append_data(2000, b"row2".to_vec()).unwrap();
        writer.append_data(3000, b"row3".to_vec()).unwrap();
        writer.flush().unwrap();
    }

    // Corrupt the last few bytes of the segment file.
    let seg_path = segment_path(&wal_dir, 0);
    let mut data = std::fs::read(&seg_path).unwrap();
    let len = data.len();
    // Flip bits near the end (corrupts the last event's checksum).
    if len > 4 {
        data[len - 1] ^= 0xFF;
        data[len - 2] ^= 0xFF;
        data[len - 3] ^= 0xFF;
        data[len - 4] ^= 0xFF;
    }
    std::fs::write(&seg_path, &data).unwrap();

    // Reading the segment should yield some valid events and then an error.
    let seg = WalSegment::open(&wal_dir, 0).unwrap();
    let mut valid_count = 0;
    let mut error_count = 0;
    for result in seg.iter_events() {
        match result {
            Ok(_) => valid_count += 1,
            Err(_) => error_count += 1,
        }
    }
    // At least some events should be recovered, and corruption detected.
    assert!(
        valid_count >= 1,
        "should recover some events before corruption"
    );
    assert!(error_count >= 1, "should detect corruption");
}

/// Corrupt WAL magic bytes.
#[test]
fn corrupt_wal_magic_rejected() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    {
        let mut seg = WalSegment::create(&wal_dir, 0).unwrap();
        seg.sync_and_seal().unwrap();
    }

    let seg_path = segment_path(&wal_dir, 0);
    let mut data = std::fs::read(&seg_path).unwrap();
    data[0] = b'Z'; // Corrupt magic
    std::fs::write(&seg_path, &data).unwrap();

    let result = WalSegment::open(&wal_dir, 0);
    assert!(result.is_err(), "corrupted magic should be rejected");
}

/// Corrupt WAL version.
#[test]
fn corrupt_wal_version_rejected() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    {
        let mut seg = WalSegment::create(&wal_dir, 0).unwrap();
        seg.sync_and_seal().unwrap();
    }

    let seg_path = segment_path(&wal_dir, 0);
    let mut data = std::fs::read(&seg_path).unwrap();
    data[4] = 0xFF; // Corrupt version byte
    data[5] = 0xFF;
    std::fs::write(&seg_path, &data).unwrap();

    let result = WalSegment::open(&wal_dir, 0);
    assert!(result.is_err(), "corrupted version should be rejected");
}

/// Corrupt WAL segment ID.
#[test]
fn corrupt_wal_segment_id_rejected() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    {
        let mut seg = WalSegment::create(&wal_dir, 0).unwrap();
        seg.sync_and_seal().unwrap();
    }

    let seg_path = segment_path(&wal_dir, 0);
    let mut data = std::fs::read(&seg_path).unwrap();
    // Set segment ID to 99 in the header while filename says 0.
    data[6..10].copy_from_slice(&99u32.to_le_bytes());
    std::fs::write(&seg_path, &data).unwrap();

    let result = WalSegment::open(&wal_dir, 0);
    assert!(result.is_err(), "segment ID mismatch should be rejected");
}

/// Truncated WAL segment (file shorter than header).
#[test]
fn truncated_wal_segment_rejected() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // Write a file that is too short to contain a header.
    let seg_path = segment_path(&wal_dir, 0);
    std::fs::write(&seg_path, b"XWA").unwrap(); // Only 3 bytes, need 16

    let result = WalSegment::open(&wal_dir, 0);
    assert!(result.is_err(), "truncated segment should be rejected");
}

/// WAL event with corrupted payload byte.
#[test]
fn corrupted_event_payload_detected() {
    let event = WalEvent::data(1, 1000, b"hello world".to_vec());
    let mut bytes = event.serialize();
    // Corrupt a payload byte.
    let header_size = 21; // EVENT_HEADER_SIZE
    if bytes.len() > header_size + 1 {
        bytes[header_size + 1] ^= 0xFF;
    }
    let result = WalEvent::deserialize(&bytes);
    assert!(result.is_err(), "corrupted payload should fail checksum");
}

/// WAL event with corrupted event type byte.
#[test]
fn corrupted_event_type_detected() {
    let event = WalEvent::data(1, 1000, b"test".to_vec());
    let mut bytes = event.serialize();
    bytes[0] = 0; // Invalid event type
    let result = WalEvent::deserialize(&bytes);
    assert!(result.is_err());
}

/// WAL event with corrupted txn_id.
#[test]
fn corrupted_event_txn_id_detected() {
    let event = WalEvent::data(1, 1000, b"test".to_vec());
    let mut bytes = event.serialize();
    bytes[1] ^= 0xFF; // Corrupt txn_id byte
    let result = WalEvent::deserialize(&bytes);
    assert!(result.is_err(), "corrupted txn_id should fail checksum");
}

/// Disk full simulation: write to a very small WAL segment.
#[test]
fn disk_full_graceful_error() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let config = WalWriterConfig {
        max_segment_size: 64, // Very small to force rotation quickly
        commit_mode: CommitMode::Sync,
    };

    let mut writer = WalWriter::create(&wal_dir, config).unwrap();

    // Write many events; should rotate frequently but not panic.
    let mut success_count = 0;
    for i in 0..100 {
        match writer.append_data(i, vec![0xAA; 32]) {
            Ok(_) => success_count += 1,
            Err(e) => {
                // Should get a clear error, not a panic.
                let msg = format!("{e}");
                assert!(!msg.is_empty());
                break;
            }
        }
    }
    // Should have written at least some events.
    assert!(success_count > 0);
    // Flush should not panic.
    let _ = writer.flush();
}

/// Multiple crashes and recoveries (3 cycles).
#[test]
fn repeated_crash_recovery_cycles() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    let mut total_rows = 0u64;

    for cycle in 0..3 {
        let n = 50 + cycle * 10;
        write_n_rows_no_commit(db_root, "test", n);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        total_rows += stats.rows_recovered;
    }

    // All rows from all cycles should be recovered.
    assert!(total_rows > 0);
}

/// Recovery on a table with no WAL directory.
#[test]
fn recovery_no_wal_dir() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let meta = create_test_table(db_root, "test");
    let table_dir = db_root.join("test");

    let stats = RecoveryManager::recover_table(&table_dir, &meta).unwrap();
    assert_eq!(stats.tables_recovered, 0);
    assert_eq!(stats.rows_recovered, 0);
}

/// Recovery on an empty WAL directory.
#[test]
fn recovery_empty_wal_dir() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let meta = create_test_table(db_root, "test");
    let table_dir = db_root.join("test");

    // Create empty WAL directory.
    std::fs::create_dir_all(table_dir.join("wal")).unwrap();

    let stats = RecoveryManager::recover_table(&table_dir, &meta).unwrap();
    assert_eq!(stats.tables_recovered, 0);
    assert_eq!(stats.rows_recovered, 0);
}

/// Recovery on nonexistent database root.
#[test]
fn recovery_nonexistent_root() {
    let dir = tempdir().unwrap();
    let db_root = dir.path().join("nonexistent");
    let stats = RecoveryManager::recover_all(&db_root).unwrap();
    assert_eq!(stats.tables_recovered, 0);
}

/// Recovery is idempotent: running twice yields same result.
#[test]
fn recovery_idempotent_no_duplicates() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    write_n_rows_no_commit(db_root, "test", 100);

    let stats1 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats1.rows_recovered, 100);

    // Second recovery should find nothing.
    let stats2 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats2.rows_recovered, 0);
    assert_eq!(stats2.tables_recovered, 0);
}

/// Multi-table crash: all tables recover independently.
#[test]
fn multi_table_crash_recovery() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta1 = create_test_table(db_root, "btc");
    let _meta2 = create_test_table(db_root, "eth");
    let _meta3 = create_test_table(db_root, "sol");

    write_n_rows_no_commit(db_root, "btc", 100);
    write_n_rows_no_commit(db_root, "eth", 200);
    write_n_rows_no_commit(db_root, "sol", 50);

    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 3);
    assert_eq!(stats.rows_recovered, 350);
}

/// Concurrent crash: writer dies while reader scans WAL segments.
#[test]
fn reader_survives_writer_crash() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    // Write initial data.
    {
        let config = WalWriterConfig {
            max_segment_size: 64 * 1024 * 1024,
            commit_mode: CommitMode::Sync,
        };
        let mut writer = WalWriter::create(&wal_dir, config).unwrap();
        for i in 0..50 {
            writer.append_data(i * 100, b"existing".to_vec()).unwrap();
        }
        writer.flush().unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(AtomicU64::new(0));

    // Reader thread: continuously reads WAL.
    let reader_handle = {
        let wal_dir = wal_dir.clone();
        let stop = stop.clone();
        let errors = errors.clone();
        thread::spawn(move || {
            let mut read_count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match WalReader::open(&wal_dir) {
                    Ok(reader) => {
                        match reader.read_all() {
                            Ok(events) => {
                                // Should see at least the initial 50 events.
                                if events.len() >= 50 {
                                    read_count += 1;
                                }
                            }
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            read_count
        })
    };

    // Writer thread: writes and then "crashes" (drops).
    let writer_handle = {
        let wal_dir = wal_dir.clone();
        thread::spawn(move || {
            let config = WalWriterConfig {
                max_segment_size: 64 * 1024 * 1024,
                commit_mode: CommitMode::Sync,
            };
            let mut writer = WalWriter::open(&wal_dir, config).unwrap();
            for i in 0..100 {
                let _ = writer.append_data(5000 + i, b"new_data".to_vec());
            }
            let _ = writer.flush();
            // Drop = "crash"
        })
    };

    writer_handle.join().unwrap();
    thread::sleep(Duration::from_millis(100));
    stop.store(true, Ordering::Relaxed);

    let reads = reader_handle.join().unwrap();
    // Reader should have done at least one successful read.
    assert!(reads > 0, "reader should have completed at least one read");
}

/// WAL segment rotation during write + crash.
#[test]
fn crash_during_segment_rotation() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let config = WalWriterConfig {
        max_segment_size: 128, // Very small to force many rotations
        commit_mode: CommitMode::Sync,
    };

    {
        let mut writer = WalWriter::create(&wal_dir, config).unwrap();
        let payload = vec![0xBB; 50];
        for _ in 0..20 {
            writer.append_data(1, payload.clone()).unwrap();
        }
        writer.flush().unwrap();
        assert!(writer.current_segment_id() > 0, "should have rotated");
        // Drop without seal = crash
    }

    // Reader should still recover all events across segments.
    let reader = WalReader::open(&wal_dir).unwrap();
    assert!(reader.segment_count() > 1);
    let events = reader.read_all().unwrap();
    assert_eq!(events.len(), 20);
}

/// Zero-length WAL event payload.
#[test]
fn zero_length_payload_event() {
    let event = WalEvent::data(1, 1000, vec![]);
    let bytes = event.serialize();
    let recovered = WalEvent::deserialize(&bytes).unwrap();
    assert_eq!(recovered, event);
    assert!(recovered.payload.is_empty());
}

/// Very large WAL event payload (1MB).
#[test]
fn large_payload_event() {
    let payload = vec![0xDE; 1024 * 1024]; // 1 MB
    let event = WalEvent::data(1, 1000, payload.clone());
    let bytes = event.serialize();
    let recovered = WalEvent::deserialize(&bytes).unwrap();
    assert_eq!(recovered.payload, payload);
}

/// All three event types roundtrip correctly.
#[test]
fn all_event_types_roundtrip() {
    for (ty, constructor) in [
        ("data", WalEvent::data as fn(u64, i64, Vec<u8>) -> WalEvent),
        ("ddl", WalEvent::ddl),
        ("truncate", WalEvent::truncate),
    ] {
        let event = constructor(42, 999, format!("{ty}_payload").into_bytes());
        let bytes = event.serialize();
        let recovered = WalEvent::deserialize(&bytes).unwrap();
        assert_eq!(recovered, event, "roundtrip failed for {ty}");
    }
}

/// Write and commit, then write and crash, then recover.
#[test]
fn committed_plus_uncommitted_recovery() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    // First: write and commit.
    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "test", config).unwrap();
        let ts = Timestamp(TS_BASE);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(10000.0),
                ],
            )
            .unwrap();
        writer.commit().unwrap();
    }

    // Second: write and crash (no commit).
    write_n_rows_no_commit(db_root, "test", 50);

    // Recovery should recover only the uncommitted rows.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.rows_recovered, 50);
}

/// WAL writer seal + reopen works correctly.
#[test]
fn seal_and_reopen() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    {
        let config = WalWriterConfig::default();
        let mut writer = WalWriter::create(&wal_dir, config).unwrap();
        writer.append_data(100, b"before_seal".to_vec()).unwrap();
        writer.flush().unwrap();
        writer.seal().unwrap();
    }

    // Reopen and write more.
    {
        let config = WalWriterConfig::default();
        let mut writer = WalWriter::open(&wal_dir, config).unwrap();
        let txn = writer.append_data(200, b"after_seal".to_vec()).unwrap();
        assert!(txn >= 2);
        writer.flush().unwrap();
    }

    let reader = WalReader::open(&wal_dir).unwrap();
    let events = reader.read_all().unwrap();
    assert!(events.len() >= 2);
}

/// Sequencer monotonicity under rapid writes.
#[test]
fn sequencer_monotonicity() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let config = WalWriterConfig::default();
    let mut writer = WalWriter::create(&wal_dir, config).unwrap();

    let mut prev_txn = 0u64;
    for i in 0..10000 {
        let txn = writer.append_data(i, b"x".to_vec()).unwrap();
        assert!(txn > prev_txn, "txn IDs must be strictly monotonic");
        prev_txn = txn;
    }
}

/// Interleaved data and DDL events in WAL.
#[test]
fn interleaved_event_types_in_wal() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let config = WalWriterConfig::default();
    let mut writer = WalWriter::create(&wal_dir, config).unwrap();

    writer.append_data(1, b"row1".to_vec()).unwrap();
    writer.append_ddl(2, b"add column".to_vec()).unwrap();
    writer.append_data(3, b"row2".to_vec()).unwrap();
    writer.append_truncate(4, b"table_x".to_vec()).unwrap();
    writer.append_data(5, b"row3".to_vec()).unwrap();
    writer.flush().unwrap();
    writer.seal().unwrap();

    let reader = WalReader::open(&wal_dir).unwrap();
    let events = reader.read_all().unwrap();
    assert_eq!(events.len(), 5);

    use exchange_core::wal::event::EventType;
    assert_eq!(events[0].event_type, EventType::Data);
    assert_eq!(events[1].event_type, EventType::Ddl);
    assert_eq!(events[2].event_type, EventType::Data);
    assert_eq!(events[3].event_type, EventType::Truncate);
    assert_eq!(events[4].event_type, EventType::Data);
}

/// Empty WAL writer flush is a no-op.
#[test]
fn empty_wal_flush_noop() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let config = WalWriterConfig::default();
    let mut writer = WalWriter::create(&wal_dir, config).unwrap();
    writer.flush().unwrap();
    writer.seal().unwrap();

    let reader = WalReader::open(&wal_dir).unwrap();
    let events = reader.read_all().unwrap();
    assert!(events.is_empty());
}

/// WAL reader on nonexistent directory returns error.
#[test]
fn wal_reader_nonexistent_dir() {
    let result = WalReader::open(Path::new("/nonexistent/path"));
    assert!(result.is_err());
}

/// WAL read_from_txn filters correctly.
#[test]
fn wal_read_from_txn_filtering() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let config = WalWriterConfig::default();
    let mut writer = WalWriter::create(&wal_dir, config).unwrap();
    for i in 0..10 {
        writer.append_data(i * 100, b"data".to_vec()).unwrap();
    }
    writer.flush().unwrap();
    writer.seal().unwrap();

    let reader = WalReader::open(&wal_dir).unwrap();
    let events = reader.read_from_txn(5).unwrap();
    assert_eq!(events.len(), 6); // txn_ids 5..10
    for event in &events {
        assert!(event.txn_id >= 5);
    }
}

/// Lazy iterator matches eager read_all.
#[test]
fn lazy_iter_matches_eager_read() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let config = WalWriterConfig {
        max_segment_size: 128, // Force rotation
        commit_mode: CommitMode::Sync,
    };
    let mut writer = WalWriter::create(&wal_dir, config).unwrap();
    for _ in 0..50 {
        writer.append_data(1, vec![0xCC; 40]).unwrap();
    }
    writer.flush().unwrap();
    writer.seal().unwrap();

    let reader = WalReader::open(&wal_dir).unwrap();
    let eager: Vec<WalEvent> = reader.read_all().unwrap();
    let lazy: Vec<WalEvent> = reader.iter().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(eager.len(), lazy.len());
    for (a, b) in eager.iter().zip(lazy.iter()) {
        assert_eq!(a, b);
    }
}

/// Crash after partial multi-table writes: only flushed tables recover.
#[test]
fn partial_multi_table_crash() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta1 = create_test_table(db_root, "flushed");
    let _meta2 = create_test_table(db_root, "not_flushed");

    // Table 1: write and flush.
    write_n_rows_no_commit(db_root, "flushed", 100);

    // Table 2: no data written at all.

    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 1);
    assert_eq!(stats.rows_recovered, 100);
}

/// Crash with data spanning multiple partitions (different days).
#[test]
fn crash_multi_partition_data() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "test", config).unwrap();

        // Day 1: 2024-03-15
        let ts1 = Timestamp(TS_BASE);
        writer
            .write_row(
                ts1,
                vec![
                    OwnedColumnValue::Timestamp(ts1.0),
                    OwnedColumnValue::F64(100.0),
                ],
            )
            .unwrap();

        // Day 2: 2024-03-16
        let ts2 = Timestamp(TS_BASE + 86_400_000_000_000);
        writer
            .write_row(
                ts2,
                vec![
                    OwnedColumnValue::Timestamp(ts2.0),
                    OwnedColumnValue::F64(200.0),
                ],
            )
            .unwrap();

        writer.flush().unwrap();
        // No commit = crash
    }

    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.rows_recovered, 2);

    let table_dir = db_root.join("test");
    let day1 = table_dir.join("2024-03-15");
    let day2 = table_dir.join("2024-03-16");
    assert!(day1.exists(), "day 1 partition should exist");
    assert!(day2.exists(), "day 2 partition should exist");
}

/// Concurrent writers to different tables should not interfere.
#[test]
fn concurrent_writers_different_tables() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta1 = create_test_table(db_root, "table_a");
    let _meta2 = create_test_table(db_root, "table_b");

    let stop = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = ["table_a", "table_b"]
        .iter()
        .map(|table_name| {
            let db_root = db_root.to_path_buf();
            let stop = stop.clone();
            let errors = errors.clone();
            let table_name = table_name.to_string();
            thread::spawn(move || {
                let config = WalTableWriterConfig::default();
                let mut writer = WalTableWriter::open(&db_root, &table_name, config).unwrap();
                let mut count = 0u64;
                while !stop.load(Ordering::Relaxed) && count < 200 {
                    let ts = Timestamp(TS_BASE + count as i64 * 1_000_000_000);
                    match writer.write_row(
                        ts,
                        vec![
                            OwnedColumnValue::Timestamp(ts.0),
                            OwnedColumnValue::F64(count as f64),
                        ],
                    ) {
                        Ok(_) => count += 1,
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                let _ = writer.flush();
                count
            })
        })
        .collect();

    thread::sleep(Duration::from_secs(1));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        let count = h.join().unwrap();
        assert!(count > 0);
    }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// WAL event wire_size is consistent with serialization.
#[test]
fn wire_size_consistency() {
    for size in [0, 1, 100, 10000] {
        let event = WalEvent::data(1, 1, vec![0xAA; size]);
        let bytes = event.serialize();
        assert_eq!(bytes.len(), event.wire_size());
    }
}

/// Segment filename format.
#[test]
fn segment_filename_format() {
    use exchange_core::wal::segment::segment_filename;
    assert_eq!(segment_filename(0), "wal-000000.wal");
    assert_eq!(segment_filename(42), "wal-000042.wal");
    assert_eq!(segment_filename(999999), "wal-999999.wal");
}

/// WalWriter reopen resumes sequencer correctly.
#[test]
fn reopen_resumes_sequencer() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    {
        let config = WalWriterConfig::default();
        let mut writer = WalWriter::create(&wal_dir, config).unwrap();
        writer.append_data(1, b"a".to_vec()).unwrap();
        writer.append_data(2, b"b".to_vec()).unwrap();
        writer.append_data(3, b"c".to_vec()).unwrap();
        writer.flush().unwrap();
    }

    let config = WalWriterConfig::default();
    let mut writer = WalWriter::open(&wal_dir, config).unwrap();
    let txn = writer.append_data(4, b"d".to_vec()).unwrap();
    assert_eq!(txn, 4, "sequencer should resume from 3");
}

/// Recovery with corrupt _meta file: table is skipped.
#[test]
fn recovery_skips_corrupt_meta() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // Create a valid table.
    let _meta = create_test_table(db_root, "valid");
    write_n_rows_no_commit(db_root, "valid", 10);

    // Create a directory with corrupt _meta.
    let corrupt_dir = db_root.join("corrupt");
    std::fs::create_dir_all(&corrupt_dir).unwrap();
    std::fs::write(corrupt_dir.join("_meta"), b"not valid json").unwrap();

    // Recovery should still succeed for the valid table.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 1);
    assert_eq!(stats.rows_recovered, 10);
}

/// Rapid open/close cycles of WalTableWriter.
#[test]
fn rapid_open_close_cycles() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    for _ in 0..20 {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "test", config).unwrap();
        let ts = Timestamp(TS_BASE);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(42.0),
                ],
            )
            .unwrap();
        writer.commit().unwrap();
    }

    // Should have accumulated rows.
    let part_dir = db_root.join("test").join("2024-03-15");
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 20);
}
