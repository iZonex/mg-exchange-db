//! WAL-to-table merge job.
//!
//! Reads committed WAL events, decodes row payloads, groups them by
//! partition, writes them into the column store, updates the `_txn` file,
//! and marks processed segments as applied.

use std::collections::HashMap;
use std::path::PathBuf;

use exchange_common::error::Result;
use exchange_common::types::{ColumnType, PartitionBy, Timestamp};

use crate::column::{FixedColumnWriter, VarColumnWriter};
use crate::partition::partition_dir;
use crate::table::TableMeta;
use crate::txn::{PartitionEntry, TxnFile, TxnHeader};

use super::event::EventType;
use super::reader::WalReader;
use super::row_codec::{OwnedColumnValue, decode_row};
use super::segment::segment_path;

/// Statistics returned by a merge run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeStats {
    pub rows_merged: u64,
    pub segments_processed: u32,
    pub partitions_touched: u32,
}

/// A job that replays WAL data events into the column store.
pub struct WalMergeJob {
    table_dir: PathBuf,
    meta: TableMeta,
}

impl WalMergeJob {
    /// Create a new merge job for the table at `table_dir` with the given metadata.
    pub fn new(table_dir: PathBuf, meta: TableMeta) -> Self {
        Self { table_dir, meta }
    }

    /// Execute the merge: read WAL events, write rows, update txn, mark segments.
    pub fn run(&self) -> Result<MergeStats> {
        let wal_dir = self.table_dir.join("wal");
        if !wal_dir.exists() {
            return Ok(MergeStats {
                rows_merged: 0,
                segments_processed: 0,
                partitions_touched: 0,
            });
        }

        let reader = WalReader::open(&wal_dir)?;
        let segment_ids = reader.segment_ids().to_vec();
        if segment_ids.is_empty() {
            return Ok(MergeStats {
                rows_merged: 0,
                segments_processed: 0,
                partitions_touched: 0,
            });
        }

        // Collect column types in schema order.
        let column_types: Vec<ColumnType> = self
            .meta
            .columns
            .iter()
            .map(|c| ColumnType::from(c.col_type))
            .collect();

        let partition_by: PartitionBy = self.meta.partition_by.into();
        let ts_col_idx = self.meta.timestamp_column;

        // Decode all data events and group rows by partition directory name.
        let mut partition_rows: HashMap<String, Vec<Vec<OwnedColumnValue>>> = HashMap::new();

        let events = reader.read_all()?;
        let mut rows_merged: u64 = 0;

        for event in &events {
            if event.event_type != EventType::Data {
                continue;
            }

            // Decode rows from the event payload.
            // Supports both batch format (row_count header) and legacy single-row format.
            let decoded_rows = match decode_event_rows(&column_types, &event.payload) {
                Ok(rows) => rows,
                Err(_) => continue,
            };

            for row in decoded_rows {
                // Extract the designated timestamp to determine partition.
                let ts_nanos = match row.get(ts_col_idx) {
                    Some(OwnedColumnValue::Timestamp(n)) => *n,
                    Some(OwnedColumnValue::I64(n)) => *n,
                    _ => event.timestamp,
                };
                let ts = Timestamp(ts_nanos);
                let dir_name = partition_dir(ts, partition_by);

                partition_rows.entry(dir_name).or_default().push(row);
                rows_merged += 1;
            }
        }

        // Write rows into each partition's column files.
        let mut global_min_ts = i64::MAX;
        let mut global_max_ts = i64::MIN;

        for (dir_name, rows) in &partition_rows {
            let part_path = self.table_dir.join(dir_name);
            std::fs::create_dir_all(&part_path)?;

            // Open column writers for this partition.
            let mut fixed_writers: HashMap<usize, FixedColumnWriter> = HashMap::new();
            let mut var_writers: HashMap<usize, VarColumnWriter> = HashMap::new();

            for (i, col_def) in self.meta.columns.iter().enumerate() {
                let ct: ColumnType = col_def.col_type.into();
                if ct.is_variable_length() {
                    let data_path = part_path.join(format!("{}.d", col_def.name));
                    let index_path = part_path.join(format!("{}.i", col_def.name));
                    var_writers.insert(i, VarColumnWriter::open(&data_path, &index_path)?);
                } else {
                    let data_path = part_path.join(format!("{}.d", col_def.name));
                    fixed_writers.insert(i, FixedColumnWriter::open(&data_path, ct)?);
                }
            }

            // Write each row.
            for row in rows {
                for (i, col_def) in self.meta.columns.iter().enumerate() {
                    let ct: ColumnType = col_def.col_type.into();
                    let val = &row[i];

                    if ct.is_variable_length() {
                        let w = var_writers.get_mut(&i).unwrap();
                        match val {
                            OwnedColumnValue::Varchar(s) => w.append_str(s)?,
                            OwnedColumnValue::Binary(b) => w.append(b)?,
                            OwnedColumnValue::Null => w.append(b"\0")?,
                            _ => w.append(b"")?,
                        }
                    } else {
                        let w = fixed_writers.get_mut(&i).unwrap();
                        write_fixed_value(w, ct, val)?;
                    }
                }

                // Track timestamp bounds.
                let ts_nanos = match &row[ts_col_idx] {
                    OwnedColumnValue::Timestamp(n) => *n,
                    OwnedColumnValue::I64(n) => *n,
                    _ => 0,
                };
                global_min_ts = global_min_ts.min(ts_nanos);
                global_max_ts = global_max_ts.max(ts_nanos);
            }

            // Flush all writers.
            for w in fixed_writers.values() {
                w.flush()?;
            }
            for w in var_writers.values() {
                w.flush()?;
            }
        }

        // Update the TxnFile.
        let mut txn = TxnFile::open(&self.table_dir)?;
        let old_hdr = txn.read_header();

        let new_row_count = old_hdr.row_count + rows_merged;
        let new_min = if old_hdr.row_count == 0 {
            global_min_ts
        } else {
            old_hdr.min_timestamp.min(global_min_ts)
        };
        let new_max = if old_hdr.row_count == 0 {
            global_max_ts
        } else {
            old_hdr.max_timestamp.max(global_max_ts)
        };

        // Build partition entries: merge existing with new counts.
        let mut part_map: HashMap<String, u64> = HashMap::new();

        // Existing partitions (we use the timestamp field as a key placeholder;
        // in a full implementation there would be a name table).
        let existing_parts = txn.read_partitions();
        for p in &existing_parts {
            // We store the timestamp as key.
            let key = format!("{}", p.timestamp);
            *part_map.entry(key).or_default() += p.row_count;
        }

        // Add new partition row counts.
        for (dir_name, rows) in &partition_rows {
            *part_map.entry(dir_name.clone()).or_default() += rows.len() as u64;
        }

        let mut part_entries: Vec<PartitionEntry> = part_map
            .into_iter()
            .map(|(key, count)| {
                let ts = key.parse::<i64>().unwrap_or(0);
                PartitionEntry {
                    timestamp: ts,
                    row_count: count,
                    name_offset: 0,
                }
            })
            .collect();
        part_entries.sort_by_key(|e| e.timestamp);

        let new_hdr = TxnHeader {
            version: old_hdr.version + 1,
            row_count: new_row_count,
            min_timestamp: new_min,
            max_timestamp: new_max,
            partition_count: part_entries.len() as u32,
        };

        txn.commit(&new_hdr, &part_entries)?;

        // Invalidate the read-only mmap cache so subsequent reads pick up
        // the newly-written column data (the cache may hold stale handles).
        crate::mmap::invalidate_mmap_cache(&self.table_dir);

        // Mark WAL segments as applied by renaming them.
        let segments_processed = segment_ids.len() as u32;
        for seg_id in &segment_ids {
            let src = segment_path(&wal_dir, *seg_id);
            let dst = src.with_extension("applied");
            if src.exists() {
                std::fs::rename(&src, &dst)?;
            }
        }

        Ok(MergeStats {
            rows_merged,
            segments_processed,
            partitions_touched: partition_rows.len() as u32,
        })
    }
}

/// Decode rows from a WAL event payload.
///
/// Supports two formats:
/// - **Batch format**: `| row_count (u32 LE) | { row_len (u32 LE) | encoded_row }* |`
/// - **Legacy single-row format**: the entire payload is a single encoded row.
///
/// Detection: if the first 4 bytes, interpreted as u32, equal 0 or 1 and the
/// payload is too short to be a batch header + row_len + row, we fall back to
/// legacy. In practice, a batch always has row_count >= 1 and the 5th-8th bytes
/// are a valid row length, so we validate by trying to parse.
fn decode_event_rows(
    column_types: &[ColumnType],
    payload: &[u8],
) -> Result<Vec<Vec<OwnedColumnValue>>> {
    // A batch payload always starts with a u32 row_count followed by
    // u32 row_len for the first row.  If the payload is >= 8 bytes and
    // the claimed row_count + lengths are consistent, treat as batch.
    if payload.len() >= 8 {
        let row_count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        // Sanity: row_count <= 10M to avoid misinterpreting legacy data.
        if row_count > 0 && row_count <= 10_000_000 {
            // Try batch decode.
            if let Ok(rows) = try_decode_batch(column_types, payload, row_count) {
                return Ok(rows);
            }
        }
    }

    // Fallback: legacy single-row format.
    let row = decode_row(column_types, payload)?;
    Ok(vec![row])
}

/// Attempt to decode a batch payload. Returns `Err` if the format is invalid.
fn try_decode_batch(
    column_types: &[ColumnType],
    payload: &[u8],
    row_count: usize,
) -> Result<Vec<Vec<OwnedColumnValue>>> {
    let mut rows = Vec::with_capacity(row_count);
    let mut offset = 4; // skip row_count header

    for _ in 0..row_count {
        if offset + 4 > payload.len() {
            return Err(exchange_common::error::ExchangeDbError::Corruption(
                "batch payload truncated (missing row length)".into(),
            ));
        }
        let row_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + row_len > payload.len() {
            return Err(exchange_common::error::ExchangeDbError::Corruption(
                "batch payload truncated (row data)".into(),
            ));
        }
        let row = decode_row(column_types, &payload[offset..offset + row_len])?;
        rows.push(row);
        offset += row_len;
    }

    Ok(rows)
}

/// Write a single fixed-width value into a FixedColumnWriter.
#[inline]
fn write_fixed_value(
    w: &mut FixedColumnWriter,
    ct: ColumnType,
    val: &OwnedColumnValue,
) -> Result<()> {
    match (ct, val) {
        (ColumnType::Boolean, OwnedColumnValue::Boolean(v)) => w.append(&[if *v { 1 } else { 0 }]),
        (ColumnType::I8, OwnedColumnValue::I8(v)) => w.append(&[*v as u8]),
        (ColumnType::I16, OwnedColumnValue::I16(v)) => w.append(&v.to_le_bytes()),
        (ColumnType::I32, OwnedColumnValue::I32(v)) => w.append_i32(*v),
        (ColumnType::I64, OwnedColumnValue::I64(v)) => w.append_i64(*v),
        (ColumnType::F32, OwnedColumnValue::F32(v)) => w.append(&v.to_le_bytes()),
        (ColumnType::F64, OwnedColumnValue::F64(v)) => w.append_f64(*v),
        (ColumnType::F64, OwnedColumnValue::Null) => w.append_f64(f64::NAN),
        (ColumnType::Timestamp, OwnedColumnValue::Timestamp(v)) => w.append_i64(*v),
        (ColumnType::Symbol, OwnedColumnValue::Symbol(v)) => w.append_i32(*v),
        (ColumnType::Uuid, OwnedColumnValue::Uuid(v)) => w.append(v),
        // Null or type mismatch: write zeros.
        _ => {
            let size = ct.fixed_size().unwrap_or(8);
            let zeroes = vec![0u8; size];
            w.append(&zeroes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::{FixedColumnReader, VarColumnReader};
    use crate::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable};
    use crate::wal::row_codec::{OwnedColumnValue, encode_row};
    use crate::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
    use tempfile::tempdir;

    fn test_meta() -> TableMeta {
        TableMeta {
            name: "test_trades".into(),
            columns: vec![
                ColumnDef {
                    name: "timestamp".into(),
                    col_type: ColumnTypeSerializable::Timestamp,
                    indexed: false,
                },
                ColumnDef {
                    name: "symbol".into(),
                    col_type: ColumnTypeSerializable::Symbol,
                    indexed: true,
                },
                ColumnDef {
                    name: "price".into(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
                ColumnDef {
                    name: "note".into(),
                    col_type: ColumnTypeSerializable::Varchar,
                    indexed: false,
                },
            ],
            partition_by: PartitionBySerializable::Day,
            timestamp_column: 0,
            version: 1,
        }
    }

    #[test]
    fn merge_wal_events_into_columns() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("test_trades");
        std::fs::create_dir_all(&table_dir).unwrap();

        let meta = test_meta();
        meta.save(&table_dir.join("_meta")).unwrap();

        // Initialize the _txn file.
        {
            let _txn = TxnFile::open(&table_dir).unwrap();
        }

        // Write WAL events.
        let wal_dir = table_dir.join("wal");
        let column_types = vec![
            ColumnType::Timestamp,
            ColumnType::Symbol,
            ColumnType::F64,
            ColumnType::Varchar,
        ];

        {
            let config = WalWriterConfig {
                max_segment_size: 64 * 1024 * 1024,
                commit_mode: CommitMode::Sync,
            };
            let mut writer = WalWriter::create(&wal_dir, config).unwrap();

            // Two rows for 2024-03-15.
            let ts1: i64 = 1_710_513_000_000_000_000; // 2024-03-15 approx
            let row1 = vec![
                OwnedColumnValue::Timestamp(ts1),
                OwnedColumnValue::Symbol(0),
                OwnedColumnValue::F64(65000.50),
                OwnedColumnValue::Varchar("buy".into()),
            ];
            let payload1 = encode_row(&column_types, &row1).unwrap();
            writer.append_data(ts1, payload1).unwrap();

            let ts2 = ts1 + 1_000_000_000; // 1 sec later, same day
            let row2 = vec![
                OwnedColumnValue::Timestamp(ts2),
                OwnedColumnValue::Symbol(1),
                OwnedColumnValue::F64(65100.25),
                OwnedColumnValue::Varchar("sell".into()),
            ];
            let payload2 = encode_row(&column_types, &row2).unwrap();
            writer.append_data(ts2, payload2).unwrap();

            writer.flush().unwrap();
        }

        // Run the merge.
        let job = WalMergeJob::new(table_dir.clone(), meta);
        let stats = job.run().unwrap();

        assert_eq!(stats.rows_merged, 2);
        assert_eq!(stats.segments_processed, 1);
        assert_eq!(stats.partitions_touched, 1);

        // Verify column files.
        let part_dir = table_dir.join("2024-03-15");
        assert!(part_dir.exists(), "partition directory should exist");

        let ts_reader =
            FixedColumnReader::open(&part_dir.join("timestamp.d"), ColumnType::Timestamp).unwrap();
        assert_eq!(ts_reader.row_count(), 2);
        assert_eq!(ts_reader.read_i64(0), 1_710_513_000_000_000_000);

        let price_reader =
            FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
        assert_eq!(price_reader.row_count(), 2);
        assert_eq!(price_reader.read_f64(0), 65000.50);
        assert_eq!(price_reader.read_f64(1), 65100.25);

        let note_reader =
            VarColumnReader::open(&part_dir.join("note.d"), &part_dir.join("note.i")).unwrap();
        assert_eq!(note_reader.row_count(), 2);
        assert_eq!(note_reader.read_str(0), "buy");
        assert_eq!(note_reader.read_str(1), "sell");

        // Verify WAL segment was marked applied.
        let applied = wal_dir.join("wal-000000.applied");
        assert!(applied.exists(), "segment should be renamed to .applied");
        let original = wal_dir.join("wal-000000.wal");
        assert!(!original.exists(), "original .wal should no longer exist");

        // Verify TxnFile was updated.
        let txn = TxnFile::open(&table_dir).unwrap();
        let hdr = txn.read_header();
        assert_eq!(hdr.version, 1);
        assert_eq!(hdr.row_count, 2);
        assert!(hdr.min_timestamp <= 1_710_513_000_000_000_000);
    }

    #[test]
    fn merge_no_wal_dir() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("no_wal_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        let meta = test_meta();
        let job = WalMergeJob::new(table_dir, meta);
        let stats = job.run().unwrap();

        assert_eq!(stats.rows_merged, 0);
        assert_eq!(stats.segments_processed, 0);
        assert_eq!(stats.partitions_touched, 0);
    }

    #[test]
    fn merge_multiple_partitions() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("multi_part");
        std::fs::create_dir_all(&table_dir).unwrap();

        let meta = TableMeta {
            name: "multi_part".into(),
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
        };
        meta.save(&table_dir.join("_meta")).unwrap();
        {
            let _txn = TxnFile::open(&table_dir).unwrap();
        }

        let wal_dir = table_dir.join("wal");
        let column_types = vec![ColumnType::Timestamp, ColumnType::I64];

        {
            let config = WalWriterConfig {
                max_segment_size: 64 * 1024 * 1024,
                commit_mode: CommitMode::Sync,
            };
            let mut writer = WalWriter::create(&wal_dir, config).unwrap();

            // Day 1: 2024-03-15
            let ts_day1: i64 = 1_710_513_000_000_000_000;
            let row = encode_row(
                &column_types,
                &[
                    OwnedColumnValue::Timestamp(ts_day1),
                    OwnedColumnValue::I64(100),
                ],
            )
            .unwrap();
            writer.append_data(ts_day1, row).unwrap();

            // Day 2: 2024-03-16 (+ 86400 seconds)
            let ts_day2: i64 = ts_day1 + 86_400_000_000_000;
            let row = encode_row(
                &column_types,
                &[
                    OwnedColumnValue::Timestamp(ts_day2),
                    OwnedColumnValue::I64(200),
                ],
            )
            .unwrap();
            writer.append_data(ts_day2, row).unwrap();

            writer.flush().unwrap();
        }

        let job = WalMergeJob::new(table_dir.clone(), meta);
        let stats = job.run().unwrap();

        assert_eq!(stats.rows_merged, 2);
        assert_eq!(stats.partitions_touched, 2);

        // Both partition directories should exist.
        assert!(table_dir.join("2024-03-15").exists());
        assert!(table_dir.join("2024-03-16").exists());
    }
}
