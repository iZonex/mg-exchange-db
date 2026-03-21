//! External sort with spill-to-disk for large result sets.
//!
//! When an in-memory sort would exceed the query memory budget, data is
//! split into sorted runs written to temporary files on disk. The final
//! result is produced by a K-way merge of all runs.

use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

use exchange_common::error::{ExchangeDbError, Result};

use crate::plan::{OrderBy, Value};

/// Rough estimate of the in-memory size of a single `Value`.
fn value_size(v: &Value) -> u64 {
    match v {
        Value::Null => 8,
        Value::I64(_) => 16,
        Value::F64(_) => 16,
        Value::Str(s) => 24 + s.len() as u64,
        Value::Timestamp(_) => 16,
    }
}

/// Estimate the in-memory size of a row.
fn row_size(row: &[Value]) -> u64 {
    let base = 24u64; // Vec overhead
    row.iter().map(value_size).sum::<u64>() + base
}

/// Serialize a Value to a JSON value for writing to a run file.
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::I64(n) => serde_json::json!({"I64": n}),
        Value::F64(n) => serde_json::json!({"F64": n}),
        Value::Str(s) => serde_json::json!({"Str": s}),
        Value::Timestamp(ns) => serde_json::json!({"Ts": ns}),
    }
}

/// Deserialize a Value from a JSON value.
fn json_to_value(j: &serde_json::Value) -> Value {
    if j.is_null() {
        return Value::Null;
    }
    if let Some(n) = j.get("I64").and_then(|v| v.as_i64()) {
        return Value::I64(n);
    }
    if let Some(n) = j.get("F64").and_then(|v| v.as_f64()) {
        return Value::F64(n);
    }
    if let Some(s) = j.get("Str").and_then(|v| v.as_str()) {
        return Value::Str(s.to_string());
    }
    if let Some(n) = j.get("Ts").and_then(|v| v.as_i64()) {
        return Value::Timestamp(n);
    }
    Value::Null
}

/// Serialize a row as a single JSON line.
fn serialize_row(row: &[Value]) -> String {
    let arr: Vec<serde_json::Value> = row.iter().map(value_to_json).collect();
    serde_json::to_string(&arr).unwrap_or_default()
}

/// Deserialize a row from a JSON line.
fn deserialize_row(line: &str) -> Result<Vec<Value>> {
    let arr: Vec<serde_json::Value> = serde_json::from_str(line).map_err(|e| {
        ExchangeDbError::Corruption(format!("failed to deserialize spill row: {e}"))
    })?;
    Ok(arr.iter().map(json_to_value).collect())
}

/// Compare two rows using the given ORDER BY specification.
fn compare_rows(a: &[Value], b: &[Value], col_indices: &[(usize, bool)]) -> std::cmp::Ordering {
    for &(idx, descending) in col_indices {
        let va = a.get(idx).unwrap_or(&Value::Null);
        let vb = b.get(idx).unwrap_or(&Value::Null);
        let cmp = va.cmp_coerce(vb).unwrap_or(std::cmp::Ordering::Equal);
        let cmp = if descending { cmp.reverse() } else { cmp };
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }
    std::cmp::Ordering::Equal
}

/// External sort: split data into sorted runs on disk, then merge.
#[allow(dead_code)]
pub struct ExternalSort {
    temp_dir: PathBuf,
    runs: Vec<PathBuf>,
    max_memory: u64,
    order_by: Vec<OrderBy>,
    /// Column names used to resolve ORDER BY references.
    col_names: Vec<String>,
    /// Resolved (index, descending) pairs, computed lazily.
    col_indices: Vec<(usize, bool)>,
    /// Current in-memory buffer.
    buffer: Vec<Vec<Value>>,
    /// Current estimated memory usage of the buffer.
    buffer_mem: u64,
    /// Counter for generating unique run file names.
    run_counter: u32,
}

impl ExternalSort {
    /// Create a new external sorter.
    ///
    /// - `temp_dir`: directory for temporary run files (must exist).
    /// - `max_memory`: maximum bytes for the in-memory sort buffer.
    /// - `order_by`: ORDER BY columns.
    /// - `col_names`: column names for resolving ORDER BY references.
    pub fn new(
        temp_dir: PathBuf,
        max_memory: u64,
        order_by: Vec<OrderBy>,
        col_names: Vec<String>,
    ) -> Self {
        let col_indices: Vec<(usize, bool)> = order_by
            .iter()
            .filter_map(|ob| {
                col_names
                    .iter()
                    .position(|n| n == &ob.column)
                    .map(|idx| (idx, ob.descending))
            })
            .collect();
        Self {
            temp_dir,
            runs: Vec::new(),
            max_memory,
            order_by,
            col_names,
            col_indices,
            buffer: Vec::new(),
            buffer_mem: 0,
            run_counter: 0,
        }
    }

    /// Add rows to the sorter. If the memory budget is exceeded, the
    /// current buffer is sorted and flushed to a run file on disk.
    pub fn add_rows(&mut self, rows: &mut Vec<Vec<Value>>) -> Result<()> {
        for row in rows.drain(..) {
            let sz = row_size(&row);
            self.buffer.push(row);
            self.buffer_mem += sz;

            if self.buffer_mem >= self.max_memory {
                self.flush_run()?;
            }
        }
        Ok(())
    }

    /// Sort and flush the current buffer to a run file.
    fn flush_run(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let col_indices = self.col_indices.clone();
        self.buffer.sort_by(|a, b| compare_rows(a, b, &col_indices));

        let run_path = self
            .temp_dir
            .join(format!("run_{}.jsonl", self.run_counter));
        self.run_counter += 1;

        let file = File::create(&run_path)?;
        let mut writer = BufWriter::new(file);
        for row in &self.buffer {
            writeln!(writer, "{}", serialize_row(row))?;
        }
        writer.flush()?;

        self.runs.push(run_path);
        self.buffer.clear();
        self.buffer_mem = 0;
        Ok(())
    }

    /// Finish sorting: merge all runs and return a sorted iterator.
    ///
    /// Any remaining rows in the buffer are flushed as a final run.
    pub fn finish(mut self) -> Result<SortedIterator> {
        // Flush any remaining rows.
        self.flush_run()?;

        if self.runs.is_empty() {
            return Ok(SortedIterator {
                runs: Vec::new(),
                heap: BinaryHeap::new(),
                col_indices: self.col_indices.clone(),
            });
        }

        // If there's only one run, just read it back.
        // For multiple runs, do a K-way merge.
        let mut run_readers = Vec::with_capacity(self.runs.len());
        let mut heap = BinaryHeap::new();

        for (i, path) in self.runs.iter().enumerate() {
            let file = File::open(path)?;
            let mut reader = RunReader {
                reader: BufReader::new(file),
                path: path.clone(),
            };
            // Prime the heap with the first row from each run.
            if let Some(row) = reader.next_row()? {
                heap.push(HeapEntry {
                    row,
                    run_index: i,
                    col_indices: self.col_indices.clone(),
                });
            }
            run_readers.push(reader);
        }

        Ok(SortedIterator {
            runs: run_readers,
            heap,
            col_indices: self.col_indices.clone(),
        })
    }

    /// How many runs have been spilled to disk.
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }
}

impl Drop for ExternalSort {
    fn drop(&mut self) {
        // Clean up run files.
        for path in &self.runs {
            let _ = fs::remove_file(path);
        }
    }
}

/// Reads rows sequentially from a sorted run file.
#[allow(dead_code)]
pub struct RunReader {
    reader: BufReader<File>,
    path: PathBuf,
}

impl RunReader {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line)?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let row = deserialize_row(line.trim())?;
        Ok(Some(row))
    }
}

/// Entry in the merge heap, ordered by the sort key.
struct HeapEntry {
    row: Vec<Value>,
    run_index: usize,
    col_indices: Vec<(usize, bool)>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        compare_rows(&self.row, &other.row, &self.col_indices) == std::cmp::Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; we want a min-heap, so reverse.
        compare_rows(&other.row, &self.row, &self.col_indices)
    }
}

/// K-way merge iterator over sorted run files.
pub struct SortedIterator {
    runs: Vec<RunReader>,
    heap: BinaryHeap<HeapEntry>,
    col_indices: Vec<(usize, bool)>,
}

impl Iterator for SortedIterator {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.heap.pop()?;
        let run_idx = entry.run_index;
        let row = entry.row;

        // Read the next row from the same run and push it onto the heap.
        match self.runs[run_idx].next_row() {
            Ok(Some(next_row)) => {
                self.heap.push(HeapEntry {
                    row: next_row,
                    run_index: run_idx,
                    col_indices: self.col_indices.clone(),
                });
            }
            Ok(None) => { /* run exhausted */ }
            Err(e) => return Some(Err(e)),
        }

        Some(Ok(row))
    }
}

impl SortedIterator {
    /// Collect all remaining rows into a Vec.
    pub fn collect_rows(self) -> Result<Vec<Vec<Value>>> {
        let mut rows = Vec::new();
        for item in self {
            rows.push(item?);
        }
        Ok(rows)
    }
}

/// Clean up run files from a finished external sort.
pub fn cleanup_runs(paths: &[PathBuf]) {
    for path in paths {
        let _ = fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(id: i64, name: &str) -> Vec<Value> {
        vec![Value::I64(id), Value::Str(name.to_string())]
    }

    #[test]
    fn external_sort_small_dataset() {
        let temp = tempfile::tempdir().unwrap();
        let order_by = vec![OrderBy {
            column: "id".to_string(),
            descending: false,
        }];
        let col_names = vec!["id".to_string(), "name".to_string()];

        let mut sorter = ExternalSort::new(
            temp.path().to_path_buf(),
            1024 * 1024, // 1MB - more than enough
            order_by,
            col_names,
        );

        let mut rows = vec![
            make_row(3, "charlie"),
            make_row(1, "alice"),
            make_row(2, "bob"),
        ];
        sorter.add_rows(&mut rows).unwrap();

        let result = sorter.finish().unwrap().collect_rows().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], Value::I64(1));
        assert_eq!(result[1][0], Value::I64(2));
        assert_eq!(result[2][0], Value::I64(3));
    }

    #[test]
    fn external_sort_spills_to_disk() {
        let temp = tempfile::tempdir().unwrap();
        let order_by = vec![OrderBy {
            column: "id".to_string(),
            descending: false,
        }];
        let col_names = vec!["id".to_string(), "value".to_string()];

        // Use a very small memory limit to force spilling.
        let mut sorter = ExternalSort::new(
            temp.path().to_path_buf(),
            200, // very small
            order_by,
            col_names,
        );

        // Add 1000 rows in reverse order.
        let mut rows: Vec<Vec<Value>> = (0..1000)
            .rev()
            .map(|i| vec![Value::I64(i), Value::Str(format!("val_{i}"))])
            .collect();
        sorter.add_rows(&mut rows).unwrap();

        // Should have created multiple runs.
        assert!(
            sorter.run_count() > 1,
            "expected multiple runs, got {}",
            sorter.run_count()
        );

        let result = sorter.finish().unwrap().collect_rows().unwrap();
        assert_eq!(result.len(), 1000);

        // Verify sorted order.
        for i in 0..1000i64 {
            assert_eq!(result[i as usize][0], Value::I64(i));
        }
    }

    #[test]
    fn external_sort_100k_rows_sorted_correctly() {
        let temp = tempfile::tempdir().unwrap();
        let order_by = vec![OrderBy {
            column: "id".to_string(),
            descending: false,
        }];
        let col_names = vec!["id".to_string(), "data".to_string()];

        let mut sorter = ExternalSort::new(
            temp.path().to_path_buf(),
            8192, // small enough to force many runs
            order_by,
            col_names,
        );

        // Create 100K rows in reverse order.
        let n = 100_000i64;
        let mut rows: Vec<Vec<Value>> = (0..n)
            .rev()
            .map(|i| vec![Value::I64(i), Value::Str(format!("r{i}"))])
            .collect();
        sorter.add_rows(&mut rows).unwrap();

        let result = sorter.finish().unwrap().collect_rows().unwrap();
        assert_eq!(result.len(), n as usize);

        for i in 0..n {
            assert_eq!(result[i as usize][0], Value::I64(i));
        }
    }

    #[test]
    fn external_sort_matches_in_memory_sort() {
        let temp = tempfile::tempdir().unwrap();
        let order_by = vec![
            OrderBy {
                column: "group".to_string(),
                descending: false,
            },
            OrderBy {
                column: "value".to_string(),
                descending: true,
            },
        ];
        let col_names = vec!["group".to_string(), "value".to_string()];

        // Generate test data.
        let mut test_data: Vec<Vec<Value>> = Vec::new();
        for i in 0..500i64 {
            test_data.push(vec![
                Value::Str(format!("g{}", i % 10)),
                Value::I64(i * 7 % 1000),
            ]);
        }

        // In-memory sort for reference.
        let mut expected = test_data.clone();
        expected.sort_by(|a, b| {
            let cmp1 = a[0].partial_cmp(&b[0]).unwrap_or(std::cmp::Ordering::Equal);
            if cmp1 != std::cmp::Ordering::Equal {
                return cmp1;
            }
            // descending for value
            b[1].partial_cmp(&a[1]).unwrap_or(std::cmp::Ordering::Equal)
        });

        // External sort with small memory budget.
        let mut sorter = ExternalSort::new(temp.path().to_path_buf(), 512, order_by, col_names);
        let mut data = test_data;
        sorter.add_rows(&mut data).unwrap();
        let result = sorter.finish().unwrap().collect_rows().unwrap();

        assert_eq!(result.len(), expected.len());
        for (i, (got, exp)) in result.iter().zip(expected.iter()).enumerate() {
            assert_eq!(got, exp, "mismatch at row {i}");
        }
    }

    #[test]
    fn external_sort_descending() {
        let temp = tempfile::tempdir().unwrap();
        let order_by = vec![OrderBy {
            column: "id".to_string(),
            descending: true,
        }];
        let col_names = vec!["id".to_string()];

        let mut sorter = ExternalSort::new(temp.path().to_path_buf(), 200, order_by, col_names);

        let mut rows: Vec<Vec<Value>> = (0..100).map(|i| vec![Value::I64(i)]).collect();
        sorter.add_rows(&mut rows).unwrap();

        let result = sorter.finish().unwrap().collect_rows().unwrap();
        assert_eq!(result.len(), 100);
        assert_eq!(result[0][0], Value::I64(99));
        assert_eq!(result[99][0], Value::I64(0));
    }

    #[test]
    fn empty_external_sort() {
        let temp = tempfile::tempdir().unwrap();
        let mut sorter = ExternalSort::new(temp.path().to_path_buf(), 1024, vec![], vec![]);
        let mut rows: Vec<Vec<Value>> = Vec::new();
        sorter.add_rows(&mut rows).unwrap();
        let result = sorter.finish().unwrap().collect_rows().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn serialization_roundtrip() {
        let row = vec![
            Value::Null,
            Value::I64(42),
            Value::F64(3.15),
            Value::Str("hello world".to_string()),
            Value::Timestamp(1_000_000_000),
        ];
        let serialized = serialize_row(&row);
        let deserialized = deserialize_row(&serialized).unwrap();
        assert_eq!(row, deserialized);
    }
}
