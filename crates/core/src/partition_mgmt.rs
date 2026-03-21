//! Partition management operations: detach, attach, split, squash.

use crate::table::{ColumnValue, TableMeta};
use exchange_common::error::{ExchangeDbError, Result};
use std::path::Path;

/// Detach a partition — rename its directory with a `.detached` suffix.
pub fn detach_partition(table_dir: &Path, partition_name: &str) -> Result<()> {
    let src = table_dir.join(partition_name);
    if !src.exists() || !src.is_dir() {
        return Err(ExchangeDbError::InvalidPartition(format!(
            "partition '{}' not found in '{}'",
            partition_name,
            table_dir.display()
        )));
    }
    let dst = table_dir.join(format!("{}.detached", partition_name));
    if dst.exists() {
        return Err(ExchangeDbError::InvalidPartition(format!(
            "partition '{}' is already detached",
            partition_name
        )));
    }
    std::fs::rename(&src, &dst)?;
    Ok(())
}

/// Attach a previously detached partition (remove `.detached` suffix).
pub fn attach_partition(table_dir: &Path, partition_name: &str) -> Result<()> {
    let src = table_dir.join(format!("{}.detached", partition_name));
    if !src.exists() || !src.is_dir() {
        return Err(ExchangeDbError::InvalidPartition(format!(
            "detached partition '{}' not found in '{}'",
            partition_name,
            table_dir.display()
        )));
    }
    let dst = table_dir.join(partition_name);
    if dst.exists() {
        return Err(ExchangeDbError::InvalidPartition(format!(
            "partition '{}' already exists (cannot attach)",
            partition_name
        )));
    }
    std::fs::rename(&src, &dst)?;
    Ok(())
}

/// List detached partitions (directories ending with `.detached`).
pub fn list_detached(table_dir: &Path) -> Result<Vec<String>> {
    let mut result = Vec::new();
    if !table_dir.exists() {
        return Ok(result);
    }
    for entry in std::fs::read_dir(table_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".detached") && entry.path().is_dir() {
            let base = name.trim_end_matches(".detached").to_string();
            result.push(base);
        }
    }
    result.sort();
    Ok(result)
}

/// Split a partition into two at a given timestamp.
///
/// Rows with timestamp < `split_ts` go into the first new partition,
/// rows with timestamp >= `split_ts` go into the second.
///
/// Returns the names of the two new partition directories.
pub fn split_partition(
    table_dir: &Path,
    partition_name: &str,
    meta: &TableMeta,
    split_ts: i64,
) -> Result<(String, String)> {
    let partition_path = table_dir.join(partition_name);
    if !partition_path.exists() || !partition_path.is_dir() {
        return Err(ExchangeDbError::InvalidPartition(format!(
            "partition '{}' not found",
            partition_name
        )));
    }

    // Read all rows from the partition.
    let rows = crate::table::read_partition_rows(&partition_path, meta)?;
    if rows.is_empty() {
        return Err(ExchangeDbError::InvalidPartition(
            "cannot split an empty partition".into(),
        ));
    }

    let ts_col = meta.timestamp_column;

    // Split rows by timestamp.
    let mut before_rows: Vec<Vec<ColumnValue<'_>>> = Vec::new();
    let mut after_rows: Vec<Vec<ColumnValue<'_>>> = Vec::new();

    for row in &rows {
        let row_ts = match &row[ts_col] {
            ColumnValue::Timestamp(t) => t.as_nanos(),
            ColumnValue::I64(v) => *v,
            _ => 0,
        };
        // Borrow each value from the owned row.
        let borrowed: Vec<ColumnValue<'_>> = row.iter().map(|v| v.borrow_column_value()).collect();
        if row_ts < split_ts {
            before_rows.push(borrowed);
        } else {
            after_rows.push(borrowed);
        }
    }

    if before_rows.is_empty() || after_rows.is_empty() {
        return Err(ExchangeDbError::InvalidPartition(
            "split timestamp does not divide the partition into two non-empty halves".into(),
        ));
    }

    let part1_name = format!("{}_1", partition_name);
    let part2_name = format!("{}_2", partition_name);
    let part1_path = table_dir.join(&part1_name);
    let part2_path = table_dir.join(&part2_name);

    std::fs::create_dir_all(&part1_path)?;
    std::fs::create_dir_all(&part2_path)?;

    crate::table::rewrite_partition(&part1_path, meta, &before_rows)?;
    crate::table::rewrite_partition(&part2_path, meta, &after_rows)?;

    // Remove original partition.
    std::fs::remove_dir_all(&partition_path)?;

    Ok((part1_name, part2_name))
}

/// Squash (merge) two partitions into one. Data from both partitions is
/// combined and sorted by the designated timestamp column.
///
/// Returns the name of the merged partition (uses `part1` name).
pub fn squash_partitions(
    table_dir: &Path,
    part1: &str,
    part2: &str,
    meta: &TableMeta,
) -> Result<String> {
    let path1 = table_dir.join(part1);
    let path2 = table_dir.join(part2);

    if !path1.exists() || !path1.is_dir() {
        return Err(ExchangeDbError::InvalidPartition(format!(
            "partition '{}' not found",
            part1
        )));
    }
    if !path2.exists() || !path2.is_dir() {
        return Err(ExchangeDbError::InvalidPartition(format!(
            "partition '{}' not found",
            part2
        )));
    }

    let mut rows1 = crate::table::read_partition_rows(&path1, meta)?;
    let rows2 = crate::table::read_partition_rows(&path2, meta)?;

    rows1.extend(rows2);

    // Sort by timestamp column.
    let ts_col = meta.timestamp_column;
    rows1.sort_by(|a, b| {
        let ts_a = match &a[ts_col] {
            ColumnValue::Timestamp(t) => t.as_nanos(),
            ColumnValue::I64(v) => *v,
            _ => 0,
        };
        let ts_b = match &b[ts_col] {
            ColumnValue::Timestamp(t) => t.as_nanos(),
            ColumnValue::I64(v) => *v,
            _ => 0,
        };
        ts_a.cmp(&ts_b)
    });

    // Write merged data into a new directory, then swap.
    let merged_name = part1.to_string();
    let merged_tmp = table_dir.join(format!("_merge_tmp_{}", part1));
    std::fs::create_dir_all(&merged_tmp)?;

    let borrowed: Vec<Vec<ColumnValue<'_>>> = rows1
        .iter()
        .map(|row| row.iter().map(|v| v.borrow_column_value()).collect())
        .collect();
    crate::table::rewrite_partition(&merged_tmp, meta, &borrowed)?;

    // Remove originals.
    std::fs::remove_dir_all(&path1)?;
    std::fs::remove_dir_all(&path2)?;

    // Rename tmp -> part1.
    std::fs::rename(&merged_tmp, table_dir.join(&merged_name))?;

    // Invalidate caches — partition files changed.
    crate::mmap::invalidate_mmap_cache(table_dir);

    Ok(merged_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{TableBuilder, TableWriter};
    use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
    use tempfile::tempdir;

    fn create_test_table(db_root: &Path) -> TableMeta {
        TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap()
    }

    fn write_rows(db_root: &Path, timestamps_secs: &[i64], prices: &[f64]) {
        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        for (ts, price) in timestamps_secs.iter().zip(prices.iter()) {
            writer
                .write_row(
                    Timestamp::from_secs(*ts),
                    &[ColumnValue::F64(*price)],
                )
                .unwrap();
        }
        writer.flush().unwrap();
    }

    #[test]
    fn detach_attach_roundtrip() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_test_table(db_root);

        // Write data for 2024-03-15.
        write_rows(db_root, &[1710513000], &[100.0]);

        let table_dir = db_root.join("trades");
        let partition_dir = table_dir.join("2024-03-15");
        assert!(partition_dir.exists());

        // Detach.
        detach_partition(&table_dir, "2024-03-15").unwrap();
        assert!(!partition_dir.exists());
        assert!(table_dir.join("2024-03-15.detached").exists());

        // List detached.
        let detached = list_detached(&table_dir).unwrap();
        assert_eq!(detached, vec!["2024-03-15".to_string()]);

        // Attach.
        attach_partition(&table_dir, "2024-03-15").unwrap();
        assert!(partition_dir.exists());
        assert!(!table_dir.join("2024-03-15.detached").exists());

        let detached = list_detached(&table_dir).unwrap();
        assert!(detached.is_empty());
    }

    #[test]
    fn detach_nonexistent_fails() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_test_table(db_root);
        let table_dir = db_root.join("trades");

        let result = detach_partition(&table_dir, "2099-01-01");
        assert!(result.is_err());
    }

    #[test]
    fn attach_nonexistent_fails() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_test_table(db_root);
        let table_dir = db_root.join("trades");

        let result = attach_partition(&table_dir, "2099-01-01");
        assert!(result.is_err());
    }

    #[test]
    fn split_partition_produces_correct_halves() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let meta = create_test_table(db_root);

        // Write 4 rows on the same day with different timestamps.
        let base = 1710513000i64; // 2024-03-15 14:30:00 UTC
        write_rows(
            db_root,
            &[base, base + 1000, base + 2000, base + 3000],
            &[100.0, 200.0, 300.0, 400.0],
        );

        let table_dir = db_root.join("trades");
        let split_ts = Timestamp::from_secs(base + 2000).as_nanos();

        let (p1, p2) =
            split_partition(&table_dir, "2024-03-15", &meta, split_ts).unwrap();

        // Read both halves and verify.
        let rows1 = crate::table::read_partition_rows(&table_dir.join(&p1), &meta).unwrap();
        let rows2 = crate::table::read_partition_rows(&table_dir.join(&p2), &meta).unwrap();

        assert_eq!(rows1.len(), 2);
        assert_eq!(rows2.len(), 2);

        // First half: prices 100.0, 200.0
        for row in &rows1 {
            match &row[1] {
                ColumnValue::F64(v) => assert!(*v == 100.0 || *v == 200.0),
                other => panic!("unexpected value: {:?}", other),
            }
        }

        // Second half: prices 300.0, 400.0
        for row in &rows2 {
            match &row[1] {
                ColumnValue::F64(v) => assert!(*v == 300.0 || *v == 400.0),
                other => panic!("unexpected value: {:?}", other),
            }
        }
    }

    #[test]
    fn squash_merges_data_correctly() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let meta = create_test_table(db_root);

        // Write data for two different days.
        let day1 = 1710513000i64; // 2024-03-15
        let day2 = 1710599400i64; // 2024-03-16
        write_rows(
            db_root,
            &[day1, day1 + 1000, day2, day2 + 1000],
            &[100.0, 200.0, 300.0, 400.0],
        );

        let table_dir = db_root.join("trades");
        assert!(table_dir.join("2024-03-15").exists());
        assert!(table_dir.join("2024-03-16").exists());

        let merged = squash_partitions(&table_dir, "2024-03-15", "2024-03-16", &meta).unwrap();
        assert_eq!(merged, "2024-03-15");

        // The merged partition should contain all 4 rows sorted by timestamp.
        let rows =
            crate::table::read_partition_rows(&table_dir.join(&merged), &meta).unwrap();
        assert_eq!(rows.len(), 4);

        // Verify they are sorted by timestamp.
        let timestamps: Vec<i64> = rows
            .iter()
            .map(|r| match &r[0] {
                ColumnValue::Timestamp(t) => t.as_nanos(),
                ColumnValue::I64(v) => *v,
                _ => 0,
            })
            .collect();
        for w in timestamps.windows(2) {
            assert!(w[0] <= w[1], "timestamps not sorted: {} > {}", w[0], w[1]);
        }

        // Second partition should be gone.
        assert!(!table_dir.join("2024-03-16").exists());
    }
}
