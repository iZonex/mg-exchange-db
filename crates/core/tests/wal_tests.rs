//! Comprehensive tests for the WAL (Write-Ahead Log) subsystem.
//!
//! 80 tests covering segments, events, writer, reader, merge, sequencer, and row codec.

use exchange_common::types::ColumnType;
use exchange_core::wal::event::{EventType, WalEvent, EVENT_HEADER_SIZE, EVENT_OVERHEAD};
use exchange_core::wal::merge::WalMergeJob;
use exchange_core::wal::reader::WalReader;
use exchange_core::wal::row_codec::{decode_row, encode_row, OwnedColumnValue};
use exchange_core::wal::segment::{WalSegment, SEGMENT_HEADER_SIZE};
use exchange_core::wal::sequencer::Sequencer;
use exchange_core::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
use tempfile::tempdir;

// ============================================================================
// WAL Segment
// ============================================================================

mod wal_segment {
    use super::*;

    #[test]
    fn create_new_segment() {
        let dir = tempdir().unwrap();
        let seg = WalSegment::create(dir.path(), 0).unwrap();
        assert_eq!(seg.segment_id(), 0);
        assert!(seg.is_empty());
        assert_eq!(seg.len(), SEGMENT_HEADER_SIZE as u64);
    }

    #[test]
    fn create_multiple_segments() {
        let dir = tempdir().unwrap();
        for id in 0..5 {
            let seg = WalSegment::create(dir.path(), id).unwrap();
            assert_eq!(seg.segment_id(), id);
        }
    }

    #[test]
    fn write_and_read_events() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 0).unwrap();
        let e1 = WalEvent::data(1, 1000, b"payload1".to_vec());
        let e2 = WalEvent::ddl(2, 2000, b"payload2".to_vec());
        seg.append_event(&e1).unwrap();
        seg.append_event(&e2).unwrap();
        seg.flush().unwrap();

        let events: Vec<WalEvent> = seg.iter_events().map(|r| r.unwrap()).collect();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], e1);
        assert_eq!(events[1], e2);
    }

    #[test]
    fn reopen_segment_preserves_data() {
        let dir = tempdir().unwrap();
        let e = WalEvent::data(1, 100, b"persist".to_vec());
        {
            let mut seg = WalSegment::create(dir.path(), 3).unwrap();
            seg.append_event(&e).unwrap();
            seg.flush().unwrap();
        }
        let seg = WalSegment::open(dir.path(), 3).unwrap();
        let events: Vec<WalEvent> = seg.iter_events().map(|r| r.unwrap()).collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], e);
    }

    #[test]
    fn empty_segment_iterates_nothing() {
        let dir = tempdir().unwrap();
        let seg = WalSegment::create(dir.path(), 0).unwrap();
        let events: Vec<_> = seg.iter_events().collect();
        assert!(events.is_empty());
    }

    #[test]
    fn bad_magic_rejected() {
        let dir = tempdir().unwrap();
        {
            let seg = WalSegment::create(dir.path(), 0).unwrap();
            seg.flush().unwrap();
        }
        let path = dir.path().join("wal-000000.wal");
        let mut data = std::fs::read(&path).unwrap();
        data[0] = b'Z';
        std::fs::write(&path, &data).unwrap();
        assert!(WalSegment::open(dir.path(), 0).is_err());
    }

    #[test]
    fn segment_data_len() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 0).unwrap();
        assert_eq!(seg.data_len(), 0);
        let e = WalEvent::data(1, 100, b"test".to_vec());
        seg.append_event(&e).unwrap();
        assert!(seg.data_len() > 0);
    }

    #[test]
    fn segment_id_mismatch_rejected() {
        let dir = tempdir().unwrap();
        {
            let seg = WalSegment::create(dir.path(), 0).unwrap();
            seg.flush().unwrap();
        }
        // Try opening as segment 1 (file has id 0)
        assert!(WalSegment::open(dir.path(), 1).is_err());
    }

    #[test]
    fn segment_sync_and_seal() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 0).unwrap();
        seg.append_event(&WalEvent::data(1, 100, b"test".to_vec()))
            .unwrap();
        seg.sync_and_seal().unwrap();
        // After seal, file should be truncated to exact data length
        let file_len = std::fs::metadata(dir.path().join("wal-000000.wal"))
            .unwrap()
            .len();
        assert!(file_len > SEGMENT_HEADER_SIZE as u64);
    }

    #[test]
    fn read_event_at_specific_offset() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 0).unwrap();
        let e1 = WalEvent::data(1, 100, b"first".to_vec());
        let e2 = WalEvent::data(2, 200, b"second".to_vec());
        seg.append_event(&e1).unwrap();
        seg.append_event(&e2).unwrap();
        seg.flush().unwrap();

        let (ev, next) = seg
            .read_event_at(SEGMENT_HEADER_SIZE as u64)
            .unwrap();
        assert_eq!(ev, e1);
        let (ev2, _) = seg.read_event_at(next).unwrap();
        assert_eq!(ev2, e2);
    }

    #[test]
    fn many_events_in_segment() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 0).unwrap();
        for i in 0..500 {
            let e = WalEvent::data(i, i as i64, format!("event_{}", i).into_bytes());
            seg.append_event(&e).unwrap();
        }
        seg.flush().unwrap();
        let events: Vec<WalEvent> = seg.iter_events().map(|r| r.unwrap()).collect();
        assert_eq!(events.len(), 500);
        assert_eq!(events[0].txn_id, 0);
        assert_eq!(events[499].txn_id, 499);
    }
}

// ============================================================================
// WAL Event
// ============================================================================

mod wal_event {
    use super::*;

    #[test]
    fn event_type_roundtrip() {
        for ty in [EventType::Data, EventType::Ddl, EventType::Truncate] {
            assert_eq!(EventType::from_u8(ty as u8).unwrap(), ty);
        }
    }

    #[test]
    fn event_type_invalid() {
        assert!(EventType::from_u8(0).is_err());
        assert!(EventType::from_u8(4).is_err());
        assert!(EventType::from_u8(255).is_err());
    }

    #[test]
    fn serialize_deserialize_data() {
        let e = WalEvent::data(42, 1_000_000_000, b"hello world".to_vec());
        let bytes = e.serialize();
        let recovered = WalEvent::deserialize(&bytes).unwrap();
        assert_eq!(recovered, e);
    }

    #[test]
    fn serialize_deserialize_ddl() {
        let e = WalEvent::ddl(1, 0, b"create table t".to_vec());
        let bytes = e.serialize();
        assert_eq!(WalEvent::deserialize(&bytes).unwrap(), e);
    }

    #[test]
    fn serialize_deserialize_truncate() {
        let e = WalEvent::truncate(5, -100, b"tablename".to_vec());
        let bytes = e.serialize();
        assert_eq!(WalEvent::deserialize(&bytes).unwrap(), e);
    }

    #[test]
    fn empty_payload() {
        let e = WalEvent::data(1, 0, vec![]);
        let bytes = e.serialize();
        assert_eq!(bytes.len(), EVENT_OVERHEAD);
        assert_eq!(WalEvent::deserialize(&bytes).unwrap(), e);
    }

    #[test]
    fn large_payload() {
        let payload = vec![0xAB; 100_000];
        let e = WalEvent::data(1, 0, payload.clone());
        let bytes = e.serialize();
        let recovered = WalEvent::deserialize(&bytes).unwrap();
        assert_eq!(recovered.payload, payload);
    }

    #[test]
    fn checksum_corruption_detected() {
        let e = WalEvent::data(1, 2, b"test".to_vec());
        let mut bytes = e.serialize();
        bytes[EVENT_HEADER_SIZE] ^= 0xFF;
        assert!(WalEvent::deserialize(&bytes).is_err());
    }

    #[test]
    fn truncated_data_detected() {
        let e = WalEvent::data(1, 2, b"test".to_vec());
        let bytes = e.serialize();
        assert!(WalEvent::deserialize(&bytes[..bytes.len() - 2]).is_err());
    }

    #[test]
    fn wire_size_correct() {
        let e = WalEvent::data(0, 0, vec![1, 2, 3, 4, 5]);
        assert_eq!(e.wire_size(), EVENT_OVERHEAD + 5);
    }

    #[test]
    fn negative_timestamp() {
        let e = WalEvent::data(1, -1_000_000_000, b"before epoch".to_vec());
        let bytes = e.serialize();
        let recovered = WalEvent::deserialize(&bytes).unwrap();
        assert_eq!(recovered.timestamp, -1_000_000_000);
    }

    #[test]
    fn max_txn_id() {
        let e = WalEvent::data(u64::MAX - 1, 0, b"max".to_vec());
        let bytes = e.serialize();
        let recovered = WalEvent::deserialize(&bytes).unwrap();
        assert_eq!(recovered.txn_id, u64::MAX - 1);
    }
}

// ============================================================================
// WAL Writer
// ============================================================================

mod wal_writer {
    use super::*;

    #[test]
    fn create_and_write() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
        let t1 = w.append_data(1000, b"row1".to_vec()).unwrap();
        let t2 = w.append_data(2000, b"row2".to_vec()).unwrap();
        assert_eq!(t1, 1);
        assert_eq!(t2, 2);
        assert_eq!(w.last_txn_id(), 2);
    }

    #[test]
    fn reopen_resumes_sequencer() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            w.append_data(100, b"a".to_vec()).unwrap();
            w.append_data(200, b"b".to_vec()).unwrap();
            w.flush().unwrap();
        }
        let mut w = WalWriter::open(&wal_dir, WalWriterConfig::default()).unwrap();
        let txn = w.append_data(300, b"c".to_vec()).unwrap();
        assert_eq!(txn, 3);
    }

    #[test]
    fn segment_rotation_small_limit() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = WalWriterConfig {
            max_segment_size: 128,
            commit_mode: CommitMode::Async,
        };
        let mut w = WalWriter::create(&wal_dir, config).unwrap();
        let payload = vec![0xAA; 64];
        for _ in 0..5 {
            w.append_data(1, payload.clone()).unwrap();
        }
        w.flush().unwrap();
        assert!(w.current_segment_id() > 0);
    }

    #[test]
    fn all_event_types() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
        let t1 = w.append_data(1, b"data".to_vec()).unwrap();
        let t2 = w.append_ddl(2, b"ddl".to_vec()).unwrap();
        let t3 = w.append_truncate(3, b"trunc".to_vec()).unwrap();
        assert_eq!(t1, 1);
        assert_eq!(t2, 2);
        assert_eq!(t3, 3);
    }

    #[test]
    fn async_commit_mode() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = WalWriterConfig {
            max_segment_size: 64 * 1024 * 1024,
            commit_mode: CommitMode::Async,
        };
        let mut w = WalWriter::create(&wal_dir, config).unwrap();
        w.append_data(1, b"async".to_vec()).unwrap();
    }

    #[test]
    fn sync_commit_mode() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = WalWriterConfig {
            max_segment_size: 64 * 1024 * 1024,
            commit_mode: CommitMode::Sync,
        };
        let mut w = WalWriter::create(&wal_dir, config).unwrap();
        w.append_data(1, b"sync".to_vec()).unwrap();
    }

    #[test]
    fn seal_writer() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
        w.append_data(1, b"before seal".to_vec()).unwrap();
        w.seal().unwrap();
    }

    #[test]
    fn many_writes() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
        for i in 0..1000 {
            w.append_data(i, format!("event_{}", i).into_bytes())
                .unwrap();
        }
        w.flush().unwrap();
        assert_eq!(w.last_txn_id(), 1000);
    }
}

// ============================================================================
// WAL Reader
// ============================================================================

mod wal_reader {
    use super::*;

    fn write_test_wal(wal_dir: &std::path::Path, n: usize) {
        let config = WalWriterConfig {
            max_segment_size: 256,
            commit_mode: CommitMode::Sync,
        };
        let mut w = WalWriter::create(wal_dir, config).unwrap();
        for i in 0..n {
            w.append_data(i as i64, format!("event_{}", i).into_bytes())
                .unwrap();
        }
        w.flush().unwrap();
    }

    #[test]
    fn read_all_events() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        write_test_wal(&wal_dir, 5);
        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_all().unwrap();
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn read_across_segments() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        write_test_wal(&wal_dir, 20);
        let reader = WalReader::open(&wal_dir).unwrap();
        assert!(reader.segment_count() > 1);
        let events = reader.read_all().unwrap();
        assert_eq!(events.len(), 20);
    }

    #[test]
    fn read_from_txn_id() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        write_test_wal(&wal_dir, 10);
        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_from_txn(5).unwrap();
        assert!(events.len() >= 6); // txn ids 5..10
        assert!(events.iter().all(|e| e.txn_id >= 5));
    }

    #[test]
    fn lazy_iterator() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        write_test_wal(&wal_dir, 15);
        let reader = WalReader::open(&wal_dir).unwrap();
        let events: Vec<WalEvent> = reader.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(events.len(), 15);
    }

    #[test]
    fn read_single_segment() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            w.append_data(1, b"hello".to_vec()).unwrap();
            w.append_data(2, b"world".to_vec()).unwrap();
            w.flush().unwrap();
        }
        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_segment(0).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn segment_count() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        write_test_wal(&wal_dir, 20);
        let reader = WalReader::open(&wal_dir).unwrap();
        assert!(reader.segment_count() >= 2);
    }

    #[test]
    fn segment_ids_sorted() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        write_test_wal(&wal_dir, 20);
        let reader = WalReader::open(&wal_dir).unwrap();
        let ids = reader.segment_ids();
        for i in 1..ids.len() {
            assert!(ids[i] > ids[i - 1]);
        }
    }

    #[test]
    fn nonexistent_dir_errors() {
        assert!(WalReader::open(std::path::Path::new("/nonexistent/wal")).is_err());
    }

    #[test]
    fn empty_wal_no_events() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        {
            let seg = WalSegment::create(&wal_dir, 0).unwrap();
            seg.flush().unwrap();
        }
        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_all().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn txn_ids_sequential() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        write_test_wal(&wal_dir, 50);
        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_all().unwrap();
        for (i, e) in events.iter().enumerate() {
            assert_eq!(e.txn_id, (i + 1) as u64);
        }
    }
}

// ============================================================================
// WAL Merge
// ============================================================================

mod wal_merge {
    use super::*;
    use exchange_core::column::FixedColumnReader;
    use exchange_core::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable, TableMeta};
    use exchange_core::txn::TxnFile;

    fn test_meta() -> TableMeta {
        TableMeta {
            name: "test_table".into(),
            columns: vec![
                ColumnDef {
                    name: "ts".into(),
                    col_type: ColumnTypeSerializable::Timestamp,
                    indexed: false,
                },
                ColumnDef {
                    name: "val".into(),
                    col_type: ColumnTypeSerializable::I64,
                    indexed: false,
                },
            ],
            partition_by: PartitionBySerializable::Day,
            timestamp_column: 0,
            version: 1,
        }
    }

    fn setup_merge(dir: &std::path::Path) -> (std::path::PathBuf, TableMeta) {
        let table_dir = dir.join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();
        let meta = test_meta();
        meta.save(&table_dir.join("_meta")).unwrap();
        { let _txn = TxnFile::open(&table_dir).unwrap(); }
        (table_dir, meta)
    }

    #[test]
    fn merge_basic() {
        let dir = tempdir().unwrap();
        let (table_dir, meta) = setup_merge(dir.path());
        let wal_dir = table_dir.join("wal");
        let col_types = vec![ColumnType::Timestamp, ColumnType::I64];
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            let ts: i64 = 1_710_513_000_000_000_000;
            let payload = encode_row(
                &col_types,
                &[OwnedColumnValue::Timestamp(ts), OwnedColumnValue::I64(100)],
            ).unwrap();
            w.append_data(ts, payload).unwrap();
            w.flush().unwrap();
        }
        let job = WalMergeJob::new(table_dir.clone(), meta);
        let stats = job.run().unwrap();
        assert_eq!(stats.rows_merged, 1);
        assert_eq!(stats.partitions_touched, 1);
    }

    #[test]
    fn merge_multiple_rows() {
        let dir = tempdir().unwrap();
        let (table_dir, meta) = setup_merge(dir.path());
        let wal_dir = table_dir.join("wal");
        let col_types = vec![ColumnType::Timestamp, ColumnType::I64];
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            let ts: i64 = 1_710_513_000_000_000_000;
            for i in 0..5 {
                let payload = encode_row(
                    &col_types,
                    &[
                        OwnedColumnValue::Timestamp(ts + i * 1_000_000_000),
                        OwnedColumnValue::I64(i),
                    ],
                ).unwrap();
                w.append_data(ts + i * 1_000_000_000, payload).unwrap();
            }
            w.flush().unwrap();
        }
        let stats = WalMergeJob::new(table_dir.clone(), meta).run().unwrap();
        assert_eq!(stats.rows_merged, 5);
    }

    #[test]
    fn merge_multiple_partitions() {
        let dir = tempdir().unwrap();
        let (table_dir, meta) = setup_merge(dir.path());
        let wal_dir = table_dir.join("wal");
        let col_types = vec![ColumnType::Timestamp, ColumnType::I64];
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            let ts_day1: i64 = 1_710_513_000_000_000_000;
            let ts_day2: i64 = ts_day1 + 86_400_000_000_000;
            let p1 = encode_row(
                &col_types,
                &[OwnedColumnValue::Timestamp(ts_day1), OwnedColumnValue::I64(1)],
            ).unwrap();
            let p2 = encode_row(
                &col_types,
                &[OwnedColumnValue::Timestamp(ts_day2), OwnedColumnValue::I64(2)],
            ).unwrap();
            w.append_data(ts_day1, p1).unwrap();
            w.append_data(ts_day2, p2).unwrap();
            w.flush().unwrap();
        }
        let stats = WalMergeJob::new(table_dir.clone(), meta).run().unwrap();
        assert_eq!(stats.rows_merged, 2);
        assert_eq!(stats.partitions_touched, 2);
    }

    #[test]
    fn merge_no_wal_dir() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("no_wal");
        std::fs::create_dir_all(&table_dir).unwrap();
        let stats = WalMergeJob::new(table_dir, test_meta()).run().unwrap();
        assert_eq!(stats.rows_merged, 0);
    }

    #[test]
    fn merge_marks_segments_applied() {
        let dir = tempdir().unwrap();
        let (table_dir, meta) = setup_merge(dir.path());
        let wal_dir = table_dir.join("wal");
        let col_types = vec![ColumnType::Timestamp, ColumnType::I64];
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            let ts: i64 = 1_710_513_000_000_000_000;
            let payload = encode_row(
                &col_types,
                &[OwnedColumnValue::Timestamp(ts), OwnedColumnValue::I64(1)],
            ).unwrap();
            w.append_data(ts, payload).unwrap();
            w.flush().unwrap();
        }
        WalMergeJob::new(table_dir.clone(), meta).run().unwrap();
        assert!(wal_dir.join("wal-000000.applied").exists());
        assert!(!wal_dir.join("wal-000000.wal").exists());
    }

    #[test]
    fn merge_updates_txn_file() {
        let dir = tempdir().unwrap();
        let (table_dir, meta) = setup_merge(dir.path());
        let wal_dir = table_dir.join("wal");
        let col_types = vec![ColumnType::Timestamp, ColumnType::I64];
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            let ts: i64 = 1_710_513_000_000_000_000;
            for i in 0..3 {
                let payload = encode_row(
                    &col_types,
                    &[
                        OwnedColumnValue::Timestamp(ts + i * 1_000_000_000),
                        OwnedColumnValue::I64(i),
                    ],
                ).unwrap();
                w.append_data(ts + i * 1_000_000_000, payload).unwrap();
            }
            w.flush().unwrap();
        }
        WalMergeJob::new(table_dir.clone(), meta).run().unwrap();
        let txn = TxnFile::open(&table_dir).unwrap();
        let hdr = txn.read_header();
        assert_eq!(hdr.row_count, 3);
        assert_eq!(hdr.version, 1);
    }

    #[test]
    fn merge_verifies_column_data() {
        let dir = tempdir().unwrap();
        let (table_dir, meta) = setup_merge(dir.path());
        let wal_dir = table_dir.join("wal");
        let col_types = vec![ColumnType::Timestamp, ColumnType::I64];
        let ts: i64 = 1_710_513_000_000_000_000;
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            let payload = encode_row(
                &col_types,
                &[OwnedColumnValue::Timestamp(ts), OwnedColumnValue::I64(42)],
            ).unwrap();
            w.append_data(ts, payload).unwrap();
            w.flush().unwrap();
        }
        WalMergeJob::new(table_dir.clone(), meta).run().unwrap();
        let r = FixedColumnReader::open(
            &table_dir.join("2024-03-15/val.d"),
            ColumnType::I64,
        )
        .unwrap();
        assert_eq!(r.read_i64(0), 42);
    }
}

// ============================================================================
// Sequencer
// ============================================================================

mod sequencer {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn monotonic_ids() {
        let seq = Sequencer::new();
        assert_eq!(seq.next_txn_id(), 1);
        assert_eq!(seq.next_txn_id(), 2);
        assert_eq!(seq.next_txn_id(), 3);
    }

    #[test]
    fn resume_from() {
        let seq = Sequencer::resume_from(100);
        assert_eq!(seq.next_txn_id(), 101);
    }

    #[test]
    fn peek_does_not_consume() {
        let seq = Sequencer::new();
        assert_eq!(seq.peek_next(), 1);
        assert_eq!(seq.peek_next(), 1);
        assert_eq!(seq.next_txn_id(), 1);
        assert_eq!(seq.peek_next(), 2);
    }

    #[test]
    fn last_txn_id() {
        let seq = Sequencer::new();
        assert_eq!(seq.last_txn_id(), 0);
        seq.next_txn_id();
        assert_eq!(seq.last_txn_id(), 1);
    }

    #[test]
    fn concurrent_unique_ids() {
        let seq = Arc::new(Sequencer::new());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let seq = Arc::clone(&seq);
                std::thread::spawn(move || {
                    let mut ids = Vec::with_capacity(1000);
                    for _ in 0..1000 {
                        ids.push(seq.next_txn_id());
                    }
                    ids
                })
            })
            .collect();
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 8000);
    }

    #[test]
    fn default_starts_at_one() {
        let seq = Sequencer::default();
        assert_eq!(seq.next_txn_id(), 1);
    }
}

// ============================================================================
// Row Codec
// ============================================================================

mod row_codec {
    use super::*;

    #[test]
    fn roundtrip_all_fixed_types() {
        let types = vec![
            ColumnType::Boolean,
            ColumnType::I8,
            ColumnType::I16,
            ColumnType::I32,
            ColumnType::I64,
            ColumnType::F32,
            ColumnType::F64,
            ColumnType::Timestamp,
            ColumnType::Symbol,
        ];
        let values = vec![
            OwnedColumnValue::Boolean(true),
            OwnedColumnValue::I8(-42),
            OwnedColumnValue::I16(1234),
            OwnedColumnValue::I32(-100_000),
            OwnedColumnValue::I64(i64::MAX),
            OwnedColumnValue::F32(3.14),
            OwnedColumnValue::F64(2.71828),
            OwnedColumnValue::Timestamp(1_710_513_000_000_000_000),
            OwnedColumnValue::Symbol(7),
        ];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_varchar_binary() {
        let types = vec![ColumnType::Varchar, ColumnType::Binary];
        let values = vec![
            OwnedColumnValue::Varchar("hello".into()),
            OwnedColumnValue::Binary(vec![0xDE, 0xAD]),
        ];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn roundtrip_nulls() {
        let types = vec![ColumnType::I64, ColumnType::Varchar, ColumnType::F64];
        let values = vec![
            OwnedColumnValue::Null,
            OwnedColumnValue::Null,
            OwnedColumnValue::Null,
        ];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn roundtrip_mixed() {
        let types = vec![
            ColumnType::Timestamp,
            ColumnType::Symbol,
            ColumnType::F64,
            ColumnType::Varchar,
        ];
        let values = vec![
            OwnedColumnValue::Timestamp(1000),
            OwnedColumnValue::Symbol(42),
            OwnedColumnValue::F64(65432.10),
            OwnedColumnValue::Varchar("BTC/USD".into()),
        ];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn empty_varchar_and_binary() {
        let types = vec![ColumnType::Varchar, ColumnType::Binary];
        let values = vec![
            OwnedColumnValue::Varchar(String::new()),
            OwnedColumnValue::Binary(vec![]),
        ];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn truncated_data_fails() {
        let types = vec![ColumnType::I64, ColumnType::F64];
        let values = vec![OwnedColumnValue::I64(1), OwnedColumnValue::F64(2.0)];
        let encoded = encode_row(&types, &values).unwrap();
        assert!(decode_row(&types, &encoded[..encoded.len() / 2]).is_err());
    }

    #[test]
    fn uuid_roundtrip() {
        let types = vec![ColumnType::Uuid];
        let values = vec![OwnedColumnValue::Uuid([1; 16])];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn long256_roundtrip() {
        let types = vec![ColumnType::Long256];
        let values = vec![OwnedColumnValue::Long256([1, 2, 3, 4])];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn geohash_roundtrip() {
        let types = vec![ColumnType::GeoHash];
        let values = vec![OwnedColumnValue::GeoHash(0x123456789ABCDEF0)];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn roundtrip_date_char_ipv4() {
        let types = vec![ColumnType::Date, ColumnType::Char, ColumnType::IPv4];
        let values = vec![
            OwnedColumnValue::Date(19800),
            OwnedColumnValue::Char(65),
            OwnedColumnValue::IPv4(0xC0A80101),
        ];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn roundtrip_long128() {
        let types = vec![ColumnType::Long128];
        let values = vec![OwnedColumnValue::Long128(i128::MAX)];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn roundtrip_boolean_false() {
        let types = vec![ColumnType::Boolean];
        let values = vec![OwnedColumnValue::Boolean(false)];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn roundtrip_all_null_row() {
        let types = vec![
            ColumnType::I32, ColumnType::I64, ColumnType::F64,
            ColumnType::Varchar, ColumnType::Binary, ColumnType::Boolean,
        ];
        let values = vec![
            OwnedColumnValue::Null, OwnedColumnValue::Null, OwnedColumnValue::Null,
            OwnedColumnValue::Null, OwnedColumnValue::Null, OwnedColumnValue::Null,
        ];
        let encoded = encode_row(&types, &values).unwrap();
        assert_eq!(decode_row(&types, &encoded).unwrap(), values);
    }

    #[test]
    fn roundtrip_large_varchar() {
        let types = vec![ColumnType::Varchar];
        let long = "x".repeat(50_000);
        let values = vec![OwnedColumnValue::Varchar(long.clone())];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        if let OwnedColumnValue::Varchar(s) = &decoded[0] {
            assert_eq!(s.len(), 50_000);
        } else {
            panic!("expected Varchar");
        }
    }

    #[test]
    fn roundtrip_large_binary() {
        let types = vec![ColumnType::Binary];
        let blob = vec![0xAB; 100_000];
        let values = vec![OwnedColumnValue::Binary(blob.clone())];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        if let OwnedColumnValue::Binary(b) = &decoded[0] {
            assert_eq!(b.len(), 100_000);
        } else {
            panic!("expected Binary");
        }
    }

    #[test]
    fn empty_data_fails_decode() {
        let types = vec![ColumnType::I64];
        assert!(decode_row(&types, &[]).is_err());
    }

    #[test]
    fn multiple_independent_rows() {
        let types = vec![ColumnType::I32, ColumnType::Varchar];
        let r1 = vec![OwnedColumnValue::I32(1), OwnedColumnValue::Varchar("a".into())];
        let r2 = vec![OwnedColumnValue::I32(2), OwnedColumnValue::Varchar("bb".into())];
        let enc1 = encode_row(&types, &r1).unwrap();
        let enc2 = encode_row(&types, &r2).unwrap();
        assert_eq!(decode_row(&types, &enc1).unwrap(), r1);
        assert_eq!(decode_row(&types, &enc2).unwrap(), r2);
    }
}

// ============================================================================
// Additional WAL writer/reader tests
// ============================================================================

mod wal_extra {
    use super::*;

    #[test]
    fn writer_last_txn_id_starts_at_zero() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
        assert_eq!(w.last_txn_id(), 0);
    }

    #[test]
    fn writer_current_segment_id_starts_at_zero() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
        assert_eq!(w.current_segment_id(), 0);
    }

    #[test]
    fn reader_read_from_txn_high_id_returns_empty() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            w.append_data(1, b"x".to_vec()).unwrap();
            w.flush().unwrap();
        }
        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_from_txn(100).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn reader_read_from_txn_one_returns_all() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            for _ in 0..5 {
                w.append_data(1, b"x".to_vec()).unwrap();
            }
            w.flush().unwrap();
        }
        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_from_txn(1).unwrap();
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn writer_large_payload() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut w = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
        let payload = vec![0xCC; 100_000];
        let txn = w.append_data(1, payload.clone()).unwrap();
        assert_eq!(txn, 1);
        w.flush().unwrap();
    }

    #[test]
    fn segment_filename_format() {
        use exchange_core::wal::segment::segment_path;
        let dir = std::path::Path::new("/tmp/wal");
        let path = segment_path(dir, 42);
        assert_eq!(
            path.to_string_lossy(),
            "/tmp/wal/wal-000042.wal"
        );
    }

    #[test]
    fn sequencer_resume_high_value() {
        let seq = Sequencer::resume_from(u64::MAX - 10);
        assert_eq!(seq.next_txn_id(), u64::MAX - 9);
    }

    #[test]
    fn sequencer_many_ids() {
        let seq = Sequencer::new();
        for i in 1..=10_000 {
            assert_eq!(seq.next_txn_id(), i);
        }
        assert_eq!(seq.last_txn_id(), 10_000);
    }
}
