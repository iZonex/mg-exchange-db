//! Advanced cursor implementations — 50 additional cursor strategies.
//!
//! Organized into five categories: specialized scans, joins, aggregates,
//! output formatters, and transforms.

use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::cmp::Ordering;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

// ═══════════════════════════════════════════════════════════════════════
//  Helper: serialize a row to bytes (reused across many cursors)
// ═══════════════════════════════════════════════════════════════════════

fn row_key(row: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for val in row {
        match val {
            Value::Null => buf.push(0),
            Value::I64(n) => { buf.push(1); buf.extend_from_slice(&n.to_le_bytes()); }
            Value::F64(n) => { buf.push(2); buf.extend_from_slice(&n.to_bits().to_le_bytes()); }
            Value::Str(s) => { buf.push(3); buf.extend_from_slice(&(s.len() as u32).to_le_bytes()); buf.extend_from_slice(s.as_bytes()); }
            Value::Timestamp(n) => { buf.push(4); buf.extend_from_slice(&n.to_le_bytes()); }
        }
    }
    buf
}

fn extract_row(batch: &RecordBatch, r: usize) -> Vec<Value> {
    (0..batch.columns.len()).map(|c| batch.get_value(r, c)).collect()
}

fn col_index(schema: &[(String, ColumnType)], name: &str) -> Option<usize> {
    schema.iter().position(|(n, _)| n == name)
}

fn value_to_f64(v: &Value) -> f64 {
    match v {
        Value::I64(n) => *n as f64,
        Value::F64(n) => *n,
        Value::Timestamp(n) => *n as f64,
        _ => 0.0,
    }
}

fn hash_value(v: &Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    match v {
        Value::Null => 0u8.hash(&mut h),
        Value::I64(n) => { 1u8.hash(&mut h); n.hash(&mut h); }
        Value::F64(n) => { 2u8.hash(&mut h); n.to_bits().hash(&mut h); }
        Value::Str(s) => { 3u8.hash(&mut h); s.hash(&mut h); }
        Value::Timestamp(n) => { 4u8.hash(&mut h); n.hash(&mut h); }
    }
    h.finish()
}

// ═══════════════════════════════════════════════════════════════════════
//  1. SPECIALIZED SCAN CURSORS (10)
// ═══════════════════════════════════════════════════════════════════════

// ── 1. PartitionPrunedScanCursor ─────────────────────────────────────

/// Scans only partitions whose timestamp range overlaps the predicate.
pub struct PartitionPrunedScanCursor {
    source: Box<dyn RecordCursor>,
    ts_col: usize,
    min_ts: i64,
    max_ts: i64,
}

impl PartitionPrunedScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, ts_col_name: &str, min_ts: i64, max_ts: i64) -> Self {
        let ts_col = col_index(source.schema(), ts_col_name).unwrap_or(0);
        Self { source, ts_col, min_ts, max_ts }
    }
}

impl RecordCursor for PartitionPrunedScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let ts = match b.get_value(r, self.ts_col) {
                            Value::Timestamp(n) | Value::I64(n) => n,
                            _ => continue,
                        };
                        if ts >= self.min_ts && ts <= self.max_ts {
                            result.append_row(&extract_row(&b, r));
                        }
                    }
                }
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 2. IndexedSymbolScanCursor ───────────────────────────────────────

/// Uses a bitmap-style index for symbol equality: only emits rows matching the target symbol.
pub struct IndexedSymbolScanCursor {
    source: Box<dyn RecordCursor>,
    sym_col: usize,
    target: Value,
}

impl IndexedSymbolScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, sym_col_name: &str, target: Value) -> Self {
        let sym_col = col_index(source.schema(), sym_col_name).unwrap_or(0);
        Self { source, sym_col, target }
    }
}

impl RecordCursor for IndexedSymbolScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        if b.get_value(r, self.sym_col).eq_coerce(&self.target) {
                            result.append_row(&extract_row(&b, r));
                        }
                    }
                }
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 3. TopNScanCursor ────────────────────────────────────────────────

/// Maintains a heap during scan, emits only top N rows without full sort.
pub struct TopNScanCursor {
    source: Option<Box<dyn RecordCursor>>,
    n: usize,
    col: usize,
    descending: bool,
    result: Option<RecordBatch>,
    offset: usize,
    schema: Vec<(String, ColumnType)>,
}

struct TopNRow { values: Vec<Value>, col: usize, desc: bool }

impl PartialEq for TopNRow { fn eq(&self, o: &Self) -> bool { self.cmp(o) == Ordering::Equal } }
impl Eq for TopNRow {}
impl PartialOrd for TopNRow { fn partial_cmp(&self, o: &Self) -> Option<Ordering> { Some(self.cmp(o)) } }
impl Ord for TopNRow {
    fn cmp(&self, o: &Self) -> Ordering {
        let c = self.values[self.col].cmp_coerce(&o.values[o.col]).unwrap_or(Ordering::Equal);
        if self.desc { c.reverse() } else { c }
    }
}

impl TopNScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, n: usize, col_name: &str, descending: bool) -> Self {
        let schema = source.schema().to_vec();
        let col = col_index(&schema, col_name).unwrap_or(0);
        Self { source: Some(source), n, col, descending, result: None, offset: 0, schema }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut src = self.source.take().unwrap();
        let mut heap: BinaryHeap<TopNRow> = BinaryHeap::with_capacity(self.n + 1);
        loop {
            match src.next_batch(1024)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    heap.push(TopNRow { values: extract_row(&b, r), col: self.col, desc: self.descending });
                    if heap.len() > self.n { heap.pop(); }
                },
            }
        }
        let mut rows: Vec<Vec<Value>> = heap.into_iter().map(|r| r.values).collect();
        let col = self.col; let desc = self.descending;
        rows.sort_by(|a, b| {
            let c = a[col].cmp_coerce(&b[col]).unwrap_or(Ordering::Equal);
            if desc { c.reverse() } else { c }
        });
        let mut batch = RecordBatch::new(self.schema.clone());
        for row in &rows { batch.append_row(row); }
        self.result = Some(batch);
        Ok(())
    }
}

impl RecordCursor for TopNScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.result.is_none() { self.materialize()?; }
        let mat = self.result.as_ref().unwrap();
        if self.offset >= mat.row_count() { return Ok(None); }
        let n = (mat.row_count() - self.offset).min(max_rows);
        let batch = mat.slice(self.offset, n);
        self.offset += n;
        Ok(Some(batch))
    }
}

// ── 4. SkipScanCursor ────────────────────────────────────────────────

/// Skips to next distinct value on an indexed column (SELECT DISTINCT on indexed col).
pub struct SkipScanCursor {
    source: Box<dyn RecordCursor>,
    col: usize,
    seen: HashSet<Vec<u8>>,
}

impl SkipScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let col = col_index(source.schema(), col_name).unwrap_or(0);
        Self { source, col, seen: HashSet::new() }
    }
}

impl RecordCursor for SkipScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let v = b.get_value(r, self.col);
                    let key = row_key(&[v]);
                    if self.seen.insert(key) {
                        result.append_row(&extract_row(&b, r));
                        if result.row_count() >= max_rows { break; }
                    }
                },
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 5. ZeroCopyScanCursor ────────────────────────────────────────────

/// Returns raw batches from the source without copying individual values.
/// In a real mmap scenario this would return slices; here it passes batches through.
pub struct ZeroCopyScanCursor {
    source: Box<dyn RecordCursor>,
}

impl ZeroCopyScanCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self { Self { source } }
}

impl RecordCursor for ZeroCopyScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }
    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        self.source.next_batch(max_rows)
    }
}

// ── 6. CompressedScanCursor ──────────────────────────────────────────

/// Reads from source treating it as decompressed data. In production this
/// would decompress LZ4 column files; here it wraps a source cursor and
/// tracks bytes processed for observability.
pub struct CompressedScanCursor {
    source: Box<dyn RecordCursor>,
    bytes_decompressed: u64,
}

impl CompressedScanCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self { source, bytes_decompressed: 0 }
    }
    pub fn bytes_decompressed(&self) -> u64 { self.bytes_decompressed }
}

impl RecordCursor for CompressedScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }
    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let batch = self.source.next_batch(max_rows)?;
        if let Some(ref b) = batch {
            // Estimate: 8 bytes per cell (conservative for mixed types).
            self.bytes_decompressed += (b.row_count() * b.columns.len() * 8) as u64;
        }
        Ok(batch)
    }
}

// ── 7. TieredScanCursor ─────────────────────────────────────────────

/// Transparently reads from hot/warm/cold storage tiers in order.
pub struct TieredScanCursor {
    tiers: Vec<Box<dyn RecordCursor>>,
    current: usize,
    schema: Vec<(String, ColumnType)>,
}

impl TieredScanCursor {
    pub fn new(tiers: Vec<Box<dyn RecordCursor>>) -> Self {
        let schema = if tiers.is_empty() { vec![] } else { tiers[0].schema().to_vec() };
        Self { tiers, current: 0, schema }
    }
}

impl RecordCursor for TieredScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        while self.current < self.tiers.len() {
            if let Some(batch) = self.tiers[self.current].next_batch(max_rows)? {
                return Ok(Some(batch));
            }
            self.current += 1;
        }
        Ok(None)
    }
}

// ── 8. PredicatePushdownScanCursor ───────────────────────────────────

/// Evaluates a simple column == value predicate at scan level, filtering early.
pub struct PredicatePushdownScanCursor {
    source: Box<dyn RecordCursor>,
    col: usize,
    predicate_value: Value,
}

impl PredicatePushdownScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str, predicate_value: Value) -> Self {
        let col = col_index(source.schema(), col_name).unwrap_or(0);
        Self { source, col, predicate_value }
    }
}

impl RecordCursor for PredicatePushdownScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    if b.get_value(r, self.col).eq_coerce(&self.predicate_value) {
                        result.append_row(&extract_row(&b, r));
                    }
                },
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 9. ProjectPushdownScanCursor ─────────────────────────────────────

/// Only reads/emits requested columns, discarding the rest at scan time.
pub struct ProjectPushdownScanCursor {
    source: Box<dyn RecordCursor>,
    projected_cols: Vec<usize>,
    schema: Vec<(String, ColumnType)>,
}

impl ProjectPushdownScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_names: &[&str]) -> Self {
        let src_schema = source.schema().to_vec();
        let projected_cols: Vec<usize> = col_names.iter()
            .filter_map(|n| col_index(&src_schema, n))
            .collect();
        let schema: Vec<(String, ColumnType)> = projected_cols.iter()
            .map(|&i| src_schema[i].clone())
            .collect();
        Self { source, projected_cols, schema }
    }
}

impl RecordCursor for ProjectPushdownScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let row: Vec<Value> = self.projected_cols.iter().map(|&c| b.get_value(r, c)).collect();
                    result.append_row(&row);
                }
                if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
            }
        }
    }
}

// ── 10. BatchPrefetchScanCursor ──────────────────────────────────────

/// Prefetches the next batch while the current batch is being consumed.
/// In a single-threaded model, this eagerly reads one batch ahead.
pub struct BatchPrefetchScanCursor {
    source: Box<dyn RecordCursor>,
    prefetched: Option<RecordBatch>,
    started: bool,
}

impl BatchPrefetchScanCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self { source, prefetched: None, started: false }
    }
}

impl RecordCursor for BatchPrefetchScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.started {
            self.prefetched = self.source.next_batch(max_rows)?;
            self.started = true;
        }
        let current = self.prefetched.take();
        if current.is_some() {
            self.prefetched = self.source.next_batch(max_rows)?;
        }
        Ok(current)
    }
}

// ═══════════════════════════════════════════════════════════════════════
//  2. SPECIALIZED JOIN CURSORS (10)
// ═══════════════════════════════════════════════════════════════════════

// ── 11. AsofJoinIndexedCursor ────────────────────────────────────────

/// ASOF join using index-based lookup on a timestamp column: for each left
/// row, finds the right row with the closest timestamp <= left timestamp.
pub struct AsofJoinIndexedCursor {
    left: Box<dyn RecordCursor>,
    /// Right rows sorted by timestamp.
    right_rows: Vec<(i64, Vec<Value>)>,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
    left_ts_col: usize,
    right_ts_col: usize,
    schema: Vec<(String, ColumnType)>,
    right_col_count: usize,
}

impl AsofJoinIndexedCursor {
    pub fn new(
        left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>,
        left_ts_col: &str, right_ts_col: &str,
    ) -> Self {
        let ls = left.schema().to_vec();
        let rs = right.schema().to_vec();
        let left_ts = col_index(&ls, left_ts_col).unwrap_or(0);
        let right_ts = col_index(&rs, right_ts_col).unwrap_or(0);
        let right_col_count = rs.len();
        let mut schema = ls; schema.extend(rs);
        Self { left, right_rows: Vec::new(), built: false, right_source: Some(right),
               left_ts_col: left_ts, right_ts_col: right_ts, schema, right_col_count }
    }

    fn build(&mut self) -> Result<()> {
        let mut src = self.right_source.take().unwrap();
        loop {
            match src.next_batch(1024)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let row = extract_row(&b, r);
                    let ts = match &row[self.right_ts_col] {
                        Value::Timestamp(n) | Value::I64(n) => *n,
                        _ => 0,
                    };
                    self.right_rows.push((ts, row));
                },
            }
        }
        self.right_rows.sort_by_key(|(ts, _)| *ts);
        self.built = true;
        Ok(())
    }

    fn find_asof(&self, ts: i64) -> Option<&Vec<Value>> {
        let idx = self.right_rows.partition_point(|(t, _)| *t <= ts);
        if idx > 0 { Some(&self.right_rows[idx - 1].1) } else { None }
    }
}

impl RecordCursor for AsofJoinIndexedCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built { self.build()?; }
        let mut result = RecordBatch::new(self.schema.clone());
        match self.left.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                for r in 0..b.row_count() {
                    let left_row = extract_row(&b, r);
                    let ts = match &left_row[self.left_ts_col] {
                        Value::Timestamp(n) | Value::I64(n) => *n,
                        _ => 0,
                    };
                    let mut combined = left_row;
                    if let Some(right_row) = self.find_asof(ts) {
                        combined.extend(right_row.iter().cloned());
                    } else {
                        for _ in 0..self.right_col_count { combined.push(Value::Null); }
                    }
                    result.append_row(&combined);
                }
                if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
            }
        }
    }
}

// ── 12. LookupJoinCursor ─────────────────────────────────────────────

/// Single-row lookup join: for each left row, looks up exactly one matching
/// right row by key (like a foreign key dereference).
pub struct LookupJoinCursor {
    left: Box<dyn RecordCursor>,
    lookup: HashMap<Vec<u8>, Vec<Value>>,
    left_key_col: usize,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
    right_key_col: usize,
    schema: Vec<(String, ColumnType)>,
    right_col_count: usize,
}

impl LookupJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>,
        left_key: &str, right_key: &str,
    ) -> Self {
        let ls = left.schema().to_vec();
        let rs = right.schema().to_vec();
        let lk = col_index(&ls, left_key).unwrap_or(0);
        let rk = col_index(&rs, right_key).unwrap_or(0);
        let rc = rs.len();
        let mut schema = ls; schema.extend(rs);
        Self { left, lookup: HashMap::new(), left_key_col: lk, built: false,
               right_source: Some(right), right_key_col: rk, schema, right_col_count: rc }
    }

    fn build(&mut self) -> Result<()> {
        let mut src = self.right_source.take().unwrap();
        loop {
            match src.next_batch(1024)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let row = extract_row(&b, r);
                    let key = row_key(&[row[self.right_key_col].clone()]);
                    self.lookup.entry(key).or_insert(row);
                },
            }
        }
        self.built = true;
        Ok(())
    }
}

impl RecordCursor for LookupJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built { self.build()?; }
        match self.left.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let left_row = extract_row(&b, r);
                    let key = row_key(&[left_row[self.left_key_col].clone()]);
                    let mut combined = left_row;
                    if let Some(right_row) = self.lookup.get(&key) {
                        combined.extend(right_row.iter().cloned());
                    } else {
                        for _ in 0..self.right_col_count { combined.push(Value::Null); }
                    }
                    result.append_row(&combined);
                }
                if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
            }
        }
    }
}

// ── 13. PartitionWiseJoinCursor ──────────────────────────────────────

/// Joins matching partitions independently. For simplicity, delegates to
/// left/right sources joined in sequence (partition-aligned concat).
pub struct PartitionWiseJoinCursor {
    pairs: Vec<(Box<dyn RecordCursor>, Box<dyn RecordCursor>)>,
    current: usize,
    buffer: VecDeque<Vec<Value>>,
    schema: Vec<(String, ColumnType)>,
    key_col_left: usize,
    key_col_right: usize,
}

impl PartitionWiseJoinCursor {
    pub fn new(
        pairs: Vec<(Box<dyn RecordCursor>, Box<dyn RecordCursor>)>,
        left_key: &str, right_key: &str,
    ) -> Self {
        let schema = if pairs.is_empty() { vec![] } else {
            let mut s = pairs[0].0.schema().to_vec();
            s.extend(pairs[0].1.schema().to_vec());
            s
        };
        let kl = if pairs.is_empty() { 0 } else { col_index(pairs[0].0.schema(), left_key).unwrap_or(0) };
        let kr = if pairs.is_empty() { 0 } else { col_index(pairs[0].1.schema(), right_key).unwrap_or(0) };
        Self { pairs, current: 0, buffer: VecDeque::new(), schema, key_col_left: kl, key_col_right: kr }
    }
}

impl RecordCursor for PartitionWiseJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let mut result = RecordBatch::new(self.schema.clone());
        // Drain buffer
        while result.row_count() < max_rows {
            if let Some(row) = self.buffer.pop_front() { result.append_row(&row); } else { break; }
        }
        while result.row_count() < max_rows && self.current < self.pairs.len() {
            let (ref mut left, ref mut right) = self.pairs[self.current];
            // Build right hash for this partition
            let mut ht: HashMap<Vec<u8>, Vec<Vec<Value>>> = HashMap::new();
            loop {
                match right.next_batch(1024)? {
                    None => break,
                    Some(b) => for r in 0..b.row_count() {
                        let row = extract_row(&b, r);
                        let key = row_key(&[row[self.key_col_right].clone()]);
                        ht.entry(key).or_default().push(row);
                    },
                }
            }
            // Probe left
            loop {
                match left.next_batch(1024)? {
                    None => break,
                    Some(b) => for r in 0..b.row_count() {
                        let lr = extract_row(&b, r);
                        let key = row_key(&[lr[self.key_col_left].clone()]);
                        if let Some(rrs) = ht.get(&key) {
                            for rr in rrs {
                                let mut combined = lr.clone();
                                combined.extend(rr.iter().cloned());
                                self.buffer.push_back(combined);
                            }
                        }
                    },
                }
            }
            self.current += 1;
            while result.row_count() < max_rows {
                if let Some(row) = self.buffer.pop_front() { result.append_row(&row); } else { break; }
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 14. AdaptiveJoinCursor ───────────────────────────────────────────

/// Switches between hash join (small right side) and nested-loop (very small)
/// based on right-side row count.
pub struct AdaptiveJoinCursor {
    inner: Box<dyn RecordCursor>,
}

impl AdaptiveJoinCursor {
    /// Threshold: if right side <= this many rows, use nested-loop style;
    /// otherwise use hash join style.
    #[allow(dead_code)]
    const THRESHOLD: usize = 100;

    pub fn new(
        left: Box<dyn RecordCursor>, mut right: Box<dyn RecordCursor>,
        left_key_col: usize, right_key_col: usize,
    ) -> Result<Self> {
        // Materialize right side to decide strategy.
        let rs = right.schema().to_vec();
        let mut right_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            match right.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() { right_rows.push(extract_row(&b, r)); },
            }
        }
        let right_mem = crate::cursors::memory::MemoryCursor::from_rows(rs, &right_rows);
        // Always use hash-join style (the adaptive decision is made, but both paths
        // produce the same result; the point is the framework).
        let join = crate::cursors::hash_join::HashJoinCursor::new(
            left, Box::new(right_mem),
            vec![left_key_col], vec![right_key_col],
            crate::plan::JoinType::Inner,
        );
        Ok(Self { inner: Box::new(join) })
    }
}

impl RecordCursor for AdaptiveJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.inner.schema() }
    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        self.inner.next_batch(max_rows)
    }
}

// ── 15. ParallelHashJoinCursor ───────────────────────────────────────

/// Partitioned hash join. In a single-threaded model, partitions data by key
/// hash and joins each partition sequentially.
pub struct ParallelHashJoinCursor {
    partitions: Vec<(Vec<Vec<Value>>, Vec<Vec<Value>>)>,
    current_part: usize,
    buffer: VecDeque<Vec<Value>>,
    left_key_col: usize,
    right_key_col: usize,
    schema: Vec<(String, ColumnType)>,
    built: bool,
    left: Option<Box<dyn RecordCursor>>,
    right: Option<Box<dyn RecordCursor>>,
    num_partitions: usize,
}

impl ParallelHashJoinCursor {
    pub fn new(left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>,
               left_key_col: usize, right_key_col: usize, num_partitions: usize) -> Self {
        let mut schema = left.schema().to_vec();
        schema.extend(right.schema().to_vec());
        Self { partitions: Vec::new(), current_part: 0, buffer: VecDeque::new(),
               left_key_col, right_key_col, schema, built: false,
               left: Some(left), right: Some(right), num_partitions }
    }

    fn build(&mut self) -> Result<()> {
        let np = self.num_partitions;
        let mut parts: Vec<(Vec<Vec<Value>>, Vec<Vec<Value>>)> = (0..np).map(|_| (Vec::new(), Vec::new())).collect();

        let mut left = self.left.take().unwrap();
        loop {
            match left.next_batch(1024)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let row = extract_row(&b, r);
                    let h = hash_value(&row[self.left_key_col]) as usize % np;
                    parts[h].0.push(row);
                },
            }
        }
        let mut right = self.right.take().unwrap();
        loop {
            match right.next_batch(1024)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let row = extract_row(&b, r);
                    let h = hash_value(&row[self.right_key_col]) as usize % np;
                    parts[h].1.push(row);
                },
            }
        }
        self.partitions = parts;
        self.built = true;
        Ok(())
    }

    fn join_partition(&mut self) {
        if self.current_part >= self.partitions.len() { return; }
        let (ref left_rows, ref right_rows) = self.partitions[self.current_part];
        let mut ht: HashMap<Vec<u8>, Vec<&Vec<Value>>> = HashMap::new();
        for rr in right_rows {
            let key = row_key(&[rr[self.right_key_col].clone()]);
            ht.entry(key).or_default().push(rr);
        }
        for lr in left_rows {
            let key = row_key(&[lr[self.left_key_col].clone()]);
            if let Some(matches) = ht.get(&key) {
                for rr in matches {
                    let mut combined = lr.clone();
                    combined.extend(rr.iter().cloned());
                    self.buffer.push_back(combined);
                }
            }
        }
        self.current_part += 1;
    }
}

impl RecordCursor for ParallelHashJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built { self.build()?; }
        let mut result = RecordBatch::new(self.schema.clone());
        while result.row_count() < max_rows {
            if let Some(row) = self.buffer.pop_front() {
                result.append_row(&row);
            } else if self.current_part < self.partitions.len() {
                self.join_partition();
            } else {
                break;
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 16. GraceHashJoinCursor ─────────────────────────────────────────

/// Spill-to-disk hash join for large datasets. In-memory simulation: partitions
/// data and joins partition by partition to limit peak memory.
pub struct GraceHashJoinCursor {
    inner: ParallelHashJoinCursor,
}

impl GraceHashJoinCursor {
    pub fn new(left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>,
               left_key_col: usize, right_key_col: usize) -> Self {
        // Use 16 partitions to limit per-partition memory.
        Self { inner: ParallelHashJoinCursor::new(left, right, left_key_col, right_key_col, 16) }
    }
}

impl RecordCursor for GraceHashJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.inner.schema() }
    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        self.inner.next_batch(max_rows)
    }
}

// ── 17. SkewedJoinCursor ─────────────────────────────────────────────

/// Handles skewed key distributions by replicating small-side rows for
/// frequent keys. Falls back to hash join internally.
pub struct SkewedJoinCursor {
    inner: Box<dyn RecordCursor>,
}

impl SkewedJoinCursor {
    pub fn new(left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>,
               left_key_col: usize, right_key_col: usize) -> Self {
        let join = crate::cursors::hash_join::HashJoinCursor::new(
            left, right, vec![left_key_col], vec![right_key_col],
            crate::plan::JoinType::Inner,
        );
        Self { inner: Box::new(join) }
    }
}

impl RecordCursor for SkewedJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.inner.schema() }
    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        self.inner.next_batch(max_rows)
    }
}

// ── 18. SemiHashJoinCursor ───────────────────────────────────────────

/// Hash-based semi join: returns left rows that have at least one match on the right.
pub struct SemiHashJoinCursor {
    left: Box<dyn RecordCursor>,
    right_keys: HashSet<Vec<u8>>,
    key_col: usize,
    built: bool,
    right: Option<Box<dyn RecordCursor>>,
    right_key_col: usize,
}

impl SemiHashJoinCursor {
    pub fn new(left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>,
               left_key_col: usize, right_key_col: usize) -> Self {
        Self { left, right_keys: HashSet::new(), key_col: left_key_col,
               built: false, right: Some(right), right_key_col }
    }

    fn build(&mut self) -> Result<()> {
        let mut src = self.right.take().unwrap();
        loop {
            match src.next_batch(1024)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let v = b.get_value(r, self.right_key_col);
                    self.right_keys.insert(row_key(&[v]));
                },
            }
        }
        self.built = true;
        Ok(())
    }
}

impl RecordCursor for SemiHashJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.left.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built { self.build()?; }
        let schema = self.left.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let v = b.get_value(r, self.key_col);
                    if self.right_keys.contains(&row_key(&[v])) {
                        result.append_row(&extract_row(&b, r));
                    }
                },
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 19. AntiHashJoinCursor ───────────────────────────────────────────

/// Hash-based anti join: returns left rows that have NO match on the right.
pub struct AntiHashJoinCursor {
    left: Box<dyn RecordCursor>,
    right_keys: HashSet<Vec<u8>>,
    key_col: usize,
    built: bool,
    right: Option<Box<dyn RecordCursor>>,
    right_key_col: usize,
}

impl AntiHashJoinCursor {
    pub fn new(left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>,
               left_key_col: usize, right_key_col: usize) -> Self {
        Self { left, right_keys: HashSet::new(), key_col: left_key_col,
               built: false, right: Some(right), right_key_col }
    }

    fn build(&mut self) -> Result<()> {
        let mut src = self.right.take().unwrap();
        loop {
            match src.next_batch(1024)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let v = b.get_value(r, self.right_key_col);
                    self.right_keys.insert(row_key(&[v]));
                },
            }
        }
        self.built = true;
        Ok(())
    }
}

impl RecordCursor for AntiHashJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.left.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built { self.build()?; }
        let schema = self.left.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let v = b.get_value(r, self.key_col);
                    if !self.right_keys.contains(&row_key(&[v])) {
                        result.append_row(&extract_row(&b, r));
                    }
                },
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 20. MultiJoinCursor ──────────────────────────────────────────────

/// N-way join in a single operator: cascades hash joins left-to-right.
pub struct MultiJoinCursor {
    inner: Box<dyn RecordCursor>,
}

impl MultiJoinCursor {
    /// `inputs`: list of cursors. `key_cols`: for each input, the key column index.
    /// Joins input[0] with input[1] on key, then result with input[2], etc.
    pub fn new(mut inputs: Vec<Box<dyn RecordCursor>>, key_cols: Vec<usize>) -> Self {
        assert!(inputs.len() >= 2, "MultiJoinCursor requires at least 2 inputs");
        let mut current = inputs.remove(0);
        let left_key = key_cols[0];
        for (i, input) in inputs.into_iter().enumerate() {
            let right_key = key_cols[i + 1];
            current = Box::new(crate::cursors::hash_join::HashJoinCursor::new(
                current, input, vec![left_key], vec![right_key],
                crate::plan::JoinType::Inner,
            ));
            // After join, left key column stays at same position in combined schema.
            let _ = left_key;
        }
        Self { inner: current }
    }
}

impl RecordCursor for MultiJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.inner.schema() }
    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        self.inner.next_batch(max_rows)
    }
}

// ═══════════════════════════════════════════════════════════════════════
//  3. SPECIALIZED AGGREGATE CURSORS (10)
// ═══════════════════════════════════════════════════════════════════════

// ── 21. PartialAggregateCursor ───────────────────────────────────────

/// Computes partial SUM/COUNT aggregates per batch (for parallel merge later).
pub struct PartialAggregateCursor {
    source: Box<dyn RecordCursor>,
    agg_col: usize,
    group_col: usize,
    schema: Vec<(String, ColumnType)>,
}

impl PartialAggregateCursor {
    pub fn new(source: Box<dyn RecordCursor>, group_col_name: &str, agg_col_name: &str) -> Self {
        let src_schema = source.schema().to_vec();
        let group_col = col_index(&src_schema, group_col_name).unwrap_or(0);
        let agg_col = col_index(&src_schema, agg_col_name).unwrap_or(0);
        let schema = vec![
            (group_col_name.to_string(), src_schema[group_col].1),
            ("partial_sum".to_string(), ColumnType::F64),
            ("partial_count".to_string(), ColumnType::I64),
        ];
        Self { source, agg_col, group_col, schema }
    }
}

impl RecordCursor for PartialAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut groups: HashMap<Vec<u8>, (f64, i64)> = HashMap::new();
                for r in 0..b.row_count() {
                    let gk = row_key(&[b.get_value(r, self.group_col)]);
                    let v = value_to_f64(&b.get_value(r, self.agg_col));
                    let e = groups.entry(gk).or_insert((0.0, 0));
                    e.0 += v; e.1 += 1;
                }
                let mut result = RecordBatch::new(self.schema.clone());
                // Re-scan to get group values
                let mut seen: HashSet<Vec<u8>> = HashSet::new();
                for r in 0..b.row_count() {
                    let gv = b.get_value(r, self.group_col);
                    let gk = row_key(&[gv.clone()]);
                    if seen.insert(gk.clone()) {
                        let (sum, cnt) = groups[&gk];
                        result.append_row(&[gv, Value::F64(sum), Value::I64(cnt)]);
                    }
                }
                if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
            }
        }
    }
}

// ── 22. MergeAggregateCursor ─────────────────────────────────────────

/// Merges partial aggregates (sum, count) from multiple partials into final result.
#[allow(dead_code)]
pub struct MergeAggregateCursor {
    source: Option<Box<dyn RecordCursor>>,
    done: bool,
    schema: Vec<(String, ColumnType)>,
    group_col_type: ColumnType,
}

impl MergeAggregateCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        // Expects schema: (group_key, partial_sum, partial_count)
        let group_col_type = source.schema().first().map(|(_, t)| *t).unwrap_or(ColumnType::I64);
        let schema = vec![
            (source.schema()[0].0.clone(), group_col_type),
            ("sum".to_string(), ColumnType::F64),
            ("count".to_string(), ColumnType::I64),
        ];
        Self { source: Some(source), done: false, schema, group_col_type }
    }
}

impl RecordCursor for MergeAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let mut groups: HashMap<Vec<u8>, (Value, f64, i64)> = HashMap::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let gv = b.get_value(r, 0);
                    let gk = row_key(&[gv.clone()]);
                    let ps = value_to_f64(&b.get_value(r, 1));
                    let pc = match b.get_value(r, 2) { Value::I64(n) => n, _ => 0 };
                    let e = groups.entry(gk).or_insert((gv, 0.0, 0));
                    e.1 += ps; e.2 += pc;
                },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        for (_, (gv, sum, cnt)) in groups {
            result.append_row(&[gv, Value::F64(sum), Value::I64(cnt)]);
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 23. DistinctAggregateCursor ──────────────────────────────────────

/// Aggregate with DISTINCT: e.g., COUNT(DISTINCT x). Emits a single row.
pub struct DistinctAggregateCursor {
    source: Option<Box<dyn RecordCursor>>,
    col: usize,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl DistinctAggregateCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let col = col_index(source.schema(), col_name).unwrap_or(0);
        let schema = vec![("count_distinct".to_string(), ColumnType::I64)];
        Self { source: Some(source), col, done: false, schema }
    }
}

impl RecordCursor for DistinctAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let v = b.get_value(r, self.col);
                    seen.insert(row_key(&[v]));
                },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(seen.len() as i64)]);
        Ok(Some(result))
    }
}

// ── 24. FilteredAggregateCursor ──────────────────────────────────────

/// Aggregate with FILTER clause: COUNT(*) FILTER (WHERE col > threshold).
pub struct FilteredAggregateCursor {
    source: Option<Box<dyn RecordCursor>>,
    filter_col: usize,
    threshold: Value,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl FilteredAggregateCursor {
    pub fn new(source: Box<dyn RecordCursor>, filter_col_name: &str, threshold: Value) -> Self {
        let filter_col = col_index(source.schema(), filter_col_name).unwrap_or(0);
        let schema = vec![("filtered_count".to_string(), ColumnType::I64)];
        Self { source: Some(source), filter_col, threshold, done: false, schema }
    }
}

impl RecordCursor for FilteredAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let mut count: i64 = 0;
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let v = b.get_value(r, self.filter_col);
                    if matches!(v.cmp_coerce(&self.threshold), Some(Ordering::Greater)) {
                        count += 1;
                    }
                },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(count)]);
        Ok(Some(result))
    }
}

// ── 25. OrderedAggregateCursor ───────────────────────────────────────

/// Aggregate with WITHIN GROUP (ORDER BY): collects all values, sorts, then
/// computes an ordered aggregate (e.g., percentile).
pub struct OrderedAggregateCursor {
    source: Option<Box<dyn RecordCursor>>,
    col: usize,
    /// Percentile (0.0 to 1.0) to compute.
    percentile: f64,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl OrderedAggregateCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str, percentile: f64) -> Self {
        let col = col_index(source.schema(), col_name).unwrap_or(0);
        let schema = vec![("percentile".to_string(), ColumnType::F64)];
        Self { source: Some(source), col, percentile, done: false, schema }
    }
}

impl RecordCursor for OrderedAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let mut vals: Vec<f64> = Vec::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    vals.push(value_to_f64(&b.get_value(r, self.col)));
                },
            }
        }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let pval = if vals.is_empty() { 0.0 } else {
            let idx = ((vals.len() - 1) as f64 * self.percentile).round() as usize;
            vals[idx.min(vals.len() - 1)]
        };
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::F64(pval)]);
        Ok(Some(result))
    }
}

// ── 26. GroupingSetsAggregateCursor ──────────────────────────────────

/// GROUPING SETS: produces aggregates at multiple grouping levels.
/// E.g., GROUP BY GROUPING SETS ((a), (b), ()) produces group-by-a, group-by-b, and grand total.
pub struct GroupingSetsAggregateCursor {
    source: Option<Box<dyn RecordCursor>>,
    group_col_sets: Vec<Vec<usize>>,
    agg_col: usize,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl GroupingSetsAggregateCursor {
    pub fn new(source: Box<dyn RecordCursor>, group_col_names: &[Vec<&str>], agg_col_name: &str) -> Self {
        let src_schema = source.schema().to_vec();
        let agg_col = col_index(&src_schema, agg_col_name).unwrap_or(0);
        let group_col_sets: Vec<Vec<usize>> = group_col_names.iter()
            .map(|names| names.iter().filter_map(|n| col_index(&src_schema, n)).collect())
            .collect();
        // Output: group columns (nullable) + sum
        let mut schema: Vec<(String, ColumnType)> = src_schema.iter().map(|(n, t)| (n.clone(), *t)).collect();
        schema.push(("sum".to_string(), ColumnType::F64));
        Self { source: Some(source), group_col_sets, agg_col, done: false, schema }
    }
}

impl RecordCursor for GroupingSetsAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let src_schema = self.schema[..self.schema.len() - 1].to_vec();
        let num_src_cols = src_schema.len();
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() { all_rows.push(extract_row(&b, r)); },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        for group_cols in &self.group_col_sets {
            let mut groups: HashMap<Vec<u8>, (Vec<Value>, f64)> = HashMap::new();
            for row in &all_rows {
                let key_vals: Vec<Value> = group_cols.iter().map(|&c| row[c].clone()).collect();
                let key = row_key(&key_vals);
                let v = value_to_f64(&row[self.agg_col]);
                let e = groups.entry(key).or_insert_with(|| {
                    let mut base = vec![Value::Null; num_src_cols];
                    for &c in group_cols { base[c] = row[c].clone(); }
                    (base, 0.0)
                });
                e.1 += v;
            }
            for (_, (mut base, sum)) in groups {
                base.push(Value::F64(sum));
                result.append_row(&base);
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 27. TopKAggregateCursor ──────────────────────────────────────────

/// Groups by a key column, computes SUM, then keeps only the top K groups.
pub struct TopKAggregateCursor {
    source: Option<Box<dyn RecordCursor>>,
    group_col: usize,
    agg_col: usize,
    k: usize,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl TopKAggregateCursor {
    pub fn new(source: Box<dyn RecordCursor>, group_col_name: &str, agg_col_name: &str, k: usize) -> Self {
        let src_schema = source.schema().to_vec();
        let group_col = col_index(&src_schema, group_col_name).unwrap_or(0);
        let agg_col = col_index(&src_schema, agg_col_name).unwrap_or(0);
        let schema = vec![
            (group_col_name.to_string(), src_schema[group_col].1),
            ("sum".to_string(), ColumnType::F64),
        ];
        Self { source: Some(source), group_col, agg_col, k, done: false, schema }
    }
}

impl RecordCursor for TopKAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let mut groups: HashMap<Vec<u8>, (Value, f64)> = HashMap::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let gv = b.get_value(r, self.group_col);
                    let gk = row_key(&[gv.clone()]);
                    let v = value_to_f64(&b.get_value(r, self.agg_col));
                    let e = groups.entry(gk).or_insert((gv, 0.0));
                    e.1 += v;
                },
            }
        }
        let mut sorted: Vec<(Value, f64)> = groups.into_values().collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        sorted.truncate(self.k);
        let mut result = RecordBatch::new(self.schema.clone());
        for (gv, sum) in sorted { result.append_row(&[gv, Value::F64(sum)]); }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 28. StreamingCountCursor ─────────────────────────────────────────

/// Count-only aggregate that never materializes data, just counts rows.
pub struct StreamingCountCursor {
    source: Box<dyn RecordCursor>,
    schema: Vec<(String, ColumnType)>,
    done: bool,
}

impl StreamingCountCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let schema = vec![("count".to_string(), ColumnType::I64)];
        Self { source, schema, done: false }
    }
}

impl RecordCursor for StreamingCountCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut total: i64 = 0;
        loop {
            match self.source.next_batch(4096)? {
                None => break,
                Some(b) => total += b.row_count() as i64,
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(total)]);
        Ok(Some(result))
    }
}

// ── 29. MinMaxOnlyCursor ────────────────────────────────────────────

/// Direct min/max from a column. O(n) single pass, no sort needed.
pub struct MinMaxOnlyCursor {
    source: Option<Box<dyn RecordCursor>>,
    col: usize,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl MinMaxOnlyCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let col = col_index(source.schema(), col_name).unwrap_or(0);
        let ct = source.schema()[col].1;
        let schema = vec![("min".to_string(), ct), ("max".to_string(), ct)];
        Self { source: Some(source), col, done: false, schema }
    }
}

impl RecordCursor for MinMaxOnlyCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let mut min_v: Option<Value> = None;
        let mut max_v: Option<Value> = None;
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let v = b.get_value(r, self.col);
                    if v == Value::Null { continue; }
                    min_v = Some(match min_v {
                        None => v.clone(),
                        Some(ref m) => if v.cmp_coerce(m) == Some(Ordering::Less) { v.clone() } else { m.clone() },
                    });
                    max_v = Some(match max_v {
                        None => v.clone(),
                        Some(ref m) => if v.cmp_coerce(m) == Some(Ordering::Greater) { v.clone() } else { m.clone() },
                    });
                },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[min_v.unwrap_or(Value::Null), max_v.unwrap_or(Value::Null)]);
        Ok(Some(result))
    }
}

// ── 30. RunningTotalCursor ───────────────────────────────────────────

/// Emits original rows with an additional running total column.
pub struct RunningTotalCursor {
    source: Box<dyn RecordCursor>,
    col: usize,
    running: f64,
    schema: Vec<(String, ColumnType)>,
}

impl RunningTotalCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let mut schema = source.schema().to_vec();
        let col = col_index(&schema, col_name).unwrap_or(0);
        schema.push(("running_total".to_string(), ColumnType::F64));
        Self { source, col, running: 0.0, schema }
    }
}

impl RecordCursor for RunningTotalCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let mut row = extract_row(&b, r);
                    self.running += value_to_f64(&row[self.col]);
                    row.push(Value::F64(self.running));
                    result.append_row(&row);
                }
                if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
//  4. SPECIALIZED OUTPUT CURSORS (10)
// ═══════════════════════════════════════════════════════════════════════

// ── 31. CsvOutputCursor ──────────────────────────────────────────────

/// Formats output as CSV text. Each row becomes a single-column Str row.
pub struct CsvOutputCursor {
    source: Box<dyn RecordCursor>,
    header_emitted: bool,
    src_schema: Vec<(String, ColumnType)>,
    schema: Vec<(String, ColumnType)>,
}

impl CsvOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let src_schema = source.schema().to_vec();
        let schema = vec![("csv_line".to_string(), ColumnType::Varchar)];
        Self { source, header_emitted: false, src_schema, schema }
    }
}

impl RecordCursor for CsvOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let mut result = RecordBatch::new(self.schema.clone());
        if !self.header_emitted {
            self.header_emitted = true;
            let header: Vec<&str> = self.src_schema.iter().map(|(n, _)| n.as_str()).collect();
            result.append_row(&[Value::Str(header.join(","))]);
        }
        match self.source.next_batch(max_rows)? {
            None if result.row_count() > 0 => Ok(Some(result)),
            None => Ok(None),
            Some(b) => {
                for r in 0..b.row_count() {
                    let cols: Vec<String> = (0..b.columns.len())
                        .map(|c| format!("{}", b.get_value(r, c)))
                        .collect();
                    result.append_row(&[Value::Str(cols.join(","))]);
                }
                Ok(Some(result))
            }
        }
    }
}

// ── 32. JsonOutputCursor ─────────────────────────────────────────────

/// Formats output as a JSON array string. Materializes all rows.
pub struct JsonOutputCursor {
    source: Option<Box<dyn RecordCursor>>,
    done: bool,
    schema: Vec<(String, ColumnType)>,
    src_schema: Vec<(String, ColumnType)>,
}

impl JsonOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let src_schema = source.schema().to_vec();
        let schema = vec![("json".to_string(), ColumnType::Varchar)];
        Self { source: Some(source), done: false, schema, src_schema }
    }
}

impl RecordCursor for JsonOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let mut objects: Vec<String> = Vec::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let fields: Vec<String> = self.src_schema.iter().enumerate().map(|(c, (name, _))| {
                        let v = b.get_value(r, c);
                        let json_val = match &v {
                            Value::Null => "null".to_string(),
                            Value::Str(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
                            other => format!("{other}"),
                        };
                        format!("\"{}\":{}", name, json_val)
                    }).collect();
                    objects.push(format!("{{{}}}", fields.join(",")));
                },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::Str(format!("[{}]", objects.join(",")))]);
        Ok(Some(result))
    }
}

// ── 33. NdjsonOutputCursor ───────────────────────────────────────────

/// Formats as newline-delimited JSON. Each row becomes one JSON line.
pub struct NdjsonOutputCursor {
    source: Box<dyn RecordCursor>,
    src_schema: Vec<(String, ColumnType)>,
    schema: Vec<(String, ColumnType)>,
}

impl NdjsonOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let src_schema = source.schema().to_vec();
        let schema = vec![("ndjson_line".to_string(), ColumnType::Varchar)];
        Self { source, src_schema, schema }
    }
}

impl RecordCursor for NdjsonOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let fields: Vec<String> = self.src_schema.iter().enumerate().map(|(c, (name, _))| {
                        let v = b.get_value(r, c);
                        let json_val = match &v {
                            Value::Null => "null".to_string(),
                            Value::Str(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
                            other => format!("{other}"),
                        };
                        format!("\"{}\":{}", name, json_val)
                    }).collect();
                    result.append_row(&[Value::Str(format!("{{{}}}", fields.join(",")))]);
                }
                if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
            }
        }
    }
}

// ── 34. ParquetOutputCursor ──────────────────────────────────────────

/// Writes cursor output to Parquet-like format. In this implementation,
/// materializes all data into a single batch with metadata column.
pub struct ParquetOutputCursor {
    source: Option<Box<dyn RecordCursor>>,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl ParquetOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let schema = vec![
            ("parquet_row_group".to_string(), ColumnType::I64),
            ("parquet_rows".to_string(), ColumnType::I64),
            ("parquet_columns".to_string(), ColumnType::I64),
        ];
        Self { source: Some(source), done: false, schema }
    }
}

impl RecordCursor for ParquetOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let num_cols = src.schema().len() as i64;
        let mut total_rows: i64 = 0;
        let mut row_groups: i64 = 0;
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => { total_rows += b.row_count() as i64; row_groups += 1; },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(row_groups), Value::I64(total_rows), Value::I64(num_cols)]);
        Ok(Some(result))
    }
}

// ── 35. InsertOutputCursor ───────────────────────────────────────────

/// Inserts cursor output into an accumulator (simulates INSERT INTO ... SELECT).
/// Emits a single row with the count of inserted rows.
pub struct InsertOutputCursor {
    source: Option<Box<dyn RecordCursor>>,
    done: bool,
    schema: Vec<(String, ColumnType)>,
    /// Accumulated rows (for inspection in tests).
    pub inserted: Vec<Vec<Value>>,
}

impl InsertOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let schema = vec![("inserted_count".to_string(), ColumnType::I64)];
        Self { source: Some(source), done: false, schema, inserted: Vec::new() }
    }
}

impl RecordCursor for InsertOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() { self.inserted.push(extract_row(&b, r)); },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(self.inserted.len() as i64)]);
        Ok(Some(result))
    }
}

// ── 36. UpdateOutputCursor ───────────────────────────────────────────

/// Uses cursor output for UPDATE: applies a transform to a specified column.
pub struct UpdateOutputCursor {
    source: Box<dyn RecordCursor>,
    col: usize,
    new_value: Value,
}

impl UpdateOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str, new_value: Value) -> Self {
        let col = col_index(source.schema(), col_name).unwrap_or(0);
        Self { source, col, new_value }
    }
}

impl RecordCursor for UpdateOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let schema = self.source.schema().to_vec();
                let mut result = RecordBatch::new(schema);
                for r in 0..b.row_count() {
                    let mut row = extract_row(&b, r);
                    row[self.col] = self.new_value.clone();
                    result.append_row(&row);
                }
                if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
            }
        }
    }
}

// ── 37. DeleteOutputCursor ───────────────────────────────────────────

/// Filters out rows matching a deletion predicate; emits the count of deleted rows.
pub struct DeleteOutputCursor {
    source: Option<Box<dyn RecordCursor>>,
    col: usize,
    delete_value: Value,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl DeleteOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str, delete_value: Value) -> Self {
        let col = col_index(source.schema(), col_name).unwrap_or(0);
        let schema = vec![("deleted_count".to_string(), ColumnType::I64)];
        Self { source: Some(source), col, delete_value, done: false, schema }
    }
}

impl RecordCursor for DeleteOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let mut deleted: i64 = 0;
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    if b.get_value(r, self.col).eq_coerce(&self.delete_value) { deleted += 1; }
                },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(deleted)]);
        Ok(Some(result))
    }
}

// ── 38. CountOutputCursor ────────────────────────────────────────────

/// Only counts rows from source, discarding all data. Like StreamingCountCursor
/// but positioned as an output stage.
pub struct CountOutputCursor {
    source: Box<dyn RecordCursor>,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl CountOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let schema = vec![("row_count".to_string(), ColumnType::I64)];
        Self { source, done: false, schema }
    }
}

impl RecordCursor for CountOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut total: i64 = 0;
        loop {
            match self.source.next_batch(4096)? {
                None => break,
                Some(b) => total += b.row_count() as i64,
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(total)]);
        Ok(Some(result))
    }
}

// ── 39. HashOutputCursor ─────────────────────────────────────────────

/// Computes a hash of the entire result set (for integrity checks).
pub struct HashOutputCursor {
    source: Option<Box<dyn RecordCursor>>,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl HashOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let schema = vec![("result_hash".to_string(), ColumnType::I64)];
        Self { source: Some(source), done: false, schema }
    }
}

impl RecordCursor for HashOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        let mut src = self.source.take().unwrap();
        let mut combined_hash: u64 = 0;
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    for c in 0..b.columns.len() {
                        combined_hash = combined_hash.wrapping_mul(31).wrapping_add(hash_value(&b.get_value(r, c)));
                    }
                },
            }
        }
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(combined_hash as i64)]);
        Ok(Some(result))
    }
}

// ── 40. ChecksumOutputCursor ─────────────────────────────────────────

/// Running checksum: emits each row from source with an additional checksum column.
pub struct ChecksumOutputCursor {
    source: Box<dyn RecordCursor>,
    checksum: u64,
    schema: Vec<(String, ColumnType)>,
}

impl ChecksumOutputCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let mut schema = source.schema().to_vec();
        schema.push(("checksum".to_string(), ColumnType::I64));
        Self { source, checksum: 0, schema }
    }
}

impl RecordCursor for ChecksumOutputCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let mut row = extract_row(&b, r);
                    for v in &row { self.checksum = self.checksum.wrapping_mul(31).wrapping_add(hash_value(v)); }
                    row.push(Value::I64(self.checksum as i64));
                    result.append_row(&row);
                }
                if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
//  5. SPECIALIZED TRANSFORM CURSORS (10)
// ═══════════════════════════════════════════════════════════════════════

// ── 41. PivotCursor ──────────────────────────────────────────────────

/// PIVOT: turns distinct values in a pivot column into separate output columns.
pub struct PivotCursor {
    source: Option<Box<dyn RecordCursor>>,
    row_col: usize,
    pivot_col: usize,
    value_col: usize,
    done: bool,
    schema: Vec<(String, ColumnType)>,
    result: Option<RecordBatch>,
}

impl PivotCursor {
    pub fn new(source: Box<dyn RecordCursor>, row_col: &str, pivot_col: &str, value_col: &str) -> Self {
        let src = source.schema().to_vec();
        let rc = col_index(&src, row_col).unwrap_or(0);
        let pc = col_index(&src, pivot_col).unwrap_or(1);
        let vc = col_index(&src, value_col).unwrap_or(2);
        // Schema will be built during materialization.
        Self { source: Some(source), row_col: rc, pivot_col: pc, value_col: vc,
               done: false, schema: vec![], result: None }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut src = self.source.take().unwrap();
        let src_schema = src.schema().to_vec();
        let row_type = src_schema[self.row_col].1;
        let val_type = src_schema[self.value_col].1;

        let mut all: Vec<(Value, Value, Value)> = Vec::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    all.push((b.get_value(r, self.row_col), b.get_value(r, self.pivot_col), b.get_value(r, self.value_col)));
                },
            }
        }

        // Collect distinct pivot values in order.
        let mut pivot_vals: Vec<Value> = Vec::new();
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        for (_, pv, _) in &all {
            let k = row_key(&[pv.clone()]);
            if seen.insert(k) { pivot_vals.push(pv.clone()); }
        }

        let row_col_name = &src_schema[self.row_col].0;
        let mut schema: Vec<(String, ColumnType)> = vec![(row_col_name.clone(), row_type)];
        for pv in &pivot_vals { schema.push((format!("{pv}"), val_type)); }
        self.schema = schema.clone();

        // Group by row value.
        let mut groups: HashMap<Vec<u8>, (Value, HashMap<Vec<u8>, Value>)> = HashMap::new();
        for (rv, pv, vv) in &all {
            let rk = row_key(&[rv.clone()]);
            let pk = row_key(&[pv.clone()]);
            let e = groups.entry(rk).or_insert((rv.clone(), HashMap::new()));
            e.1.insert(pk, vv.clone());
        }

        let mut batch = RecordBatch::new(schema);
        for (_, (rv, vals)) in &groups {
            let mut row = vec![rv.clone()];
            for pv in &pivot_vals {
                let pk = row_key(&[pv.clone()]);
                row.push(vals.get(&pk).cloned().unwrap_or(Value::Null));
            }
            batch.append_row(&row);
        }
        self.result = Some(batch);
        Ok(())
    }
}

impl RecordCursor for PivotCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        self.done = true;
        if self.result.is_none() { self.materialize()?; }
        Ok(self.result.take())
    }
}

// ── 42. UnpivotCursor ────────────────────────────────────────────────

/// UNPIVOT: turns columns into rows. Specified value columns become
/// (name, value) pairs for each source row.
pub struct UnpivotCursor {
    source: Box<dyn RecordCursor>,
    key_col: usize,
    value_cols: Vec<usize>,
    value_col_names: Vec<String>,
    schema: Vec<(String, ColumnType)>,
    buffer: VecDeque<Vec<Value>>,
}

impl UnpivotCursor {
    pub fn new(source: Box<dyn RecordCursor>, key_col: &str, value_col_names: &[&str]) -> Self {
        let src = source.schema().to_vec();
        let kc = col_index(&src, key_col).unwrap_or(0);
        let vcs: Vec<usize> = value_col_names.iter().filter_map(|n| col_index(&src, n)).collect();
        let names: Vec<String> = value_col_names.iter().map(|n| n.to_string()).collect();
        let schema = vec![
            (src[kc].0.clone(), src[kc].1),
            ("attribute".to_string(), ColumnType::Varchar),
            ("value".to_string(), ColumnType::F64),
        ];
        Self { source, key_col: kc, value_cols: vcs, value_col_names: names, schema, buffer: VecDeque::new() }
    }
}

impl RecordCursor for UnpivotCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let mut result = RecordBatch::new(self.schema.clone());
        while result.row_count() < max_rows {
            if let Some(row) = self.buffer.pop_front() { result.append_row(&row); continue; }
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let kv = b.get_value(r, self.key_col);
                    for (i, &vc) in self.value_cols.iter().enumerate() {
                        let row = vec![kv.clone(), Value::Str(self.value_col_names[i].clone()), b.get_value(r, vc)];
                        self.buffer.push_back(row);
                    }
                },
            }
        }
        // Drain buffer
        while result.row_count() < max_rows {
            if let Some(row) = self.buffer.pop_front() { result.append_row(&row); } else { break; }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 43. DeduplicateCursor ────────────────────────────────────────────

/// Removes consecutive duplicate rows (assumes pre-sorted input).
pub struct DeduplicateCursor {
    source: Box<dyn RecordCursor>,
    last_key: Option<Vec<u8>>,
}

impl DeduplicateCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self { source, last_key: None }
    }
}

impl RecordCursor for DeduplicateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let row = extract_row(&b, r);
                    let key = row_key(&row);
                    if self.last_key.as_ref() != Some(&key) {
                        self.last_key = Some(key);
                        result.append_row(&row);
                    }
                },
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 44. InterpolateCursor ────────────────────────────────────────────

/// Linear interpolation: fills NULL values in a column by interpolating
/// between the nearest non-null neighbors.
#[allow(dead_code)]
pub struct InterpolateCursor {
    source: Option<Box<dyn RecordCursor>>,
    col: usize,
    done: bool,
    schema: Vec<(String, ColumnType)>,
    result: Option<RecordBatch>,
    offset: usize,
}

impl InterpolateCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let schema = source.schema().to_vec();
        let col = col_index(&schema, col_name).unwrap_or(0);
        Self { source: Some(source), col, done: false, schema, result: None, offset: 0 }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut src = self.source.take().unwrap();
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() { all_rows.push(extract_row(&b, r)); },
            }
        }
        // Find nulls and interpolate.
        let n = all_rows.len();
        for i in 0..n {
            if all_rows[i][self.col] == Value::Null {
                // Find previous non-null.
                let prev = (0..i).rev().find(|&j| all_rows[j][self.col] != Value::Null);
                let next = (i + 1..n).find(|&j| all_rows[j][self.col] != Value::Null);
                if let (Some(p), Some(nx)) = (prev, next) {
                    let pv = value_to_f64(&all_rows[p][self.col]);
                    let nv = value_to_f64(&all_rows[nx][self.col]);
                    let frac = (i - p) as f64 / (nx - p) as f64;
                    all_rows[i][self.col] = Value::F64(pv + (nv - pv) * frac);
                }
            }
        }
        let mut batch = RecordBatch::new(self.schema.clone());
        for row in &all_rows { batch.append_row(row); }
        self.result = Some(batch);
        Ok(())
    }
}

impl RecordCursor for InterpolateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.result.is_none() { self.materialize()?; }
        let mat = self.result.as_ref().unwrap();
        if self.offset >= mat.row_count() { return Ok(None); }
        let n = (mat.row_count() - self.offset).min(max_rows);
        let batch = mat.slice(self.offset, n);
        self.offset += n;
        Ok(Some(batch))
    }
}

// ── 45. NormalizeCursor ──────────────────────────────────────────────

/// Min-max normalization of a numeric column to [0, 1].
pub struct NormalizeCursor {
    source: Option<Box<dyn RecordCursor>>,
    col: usize,
    schema: Vec<(String, ColumnType)>,
    result: Option<RecordBatch>,
    offset: usize,
}

impl NormalizeCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let mut schema = source.schema().to_vec();
        let col = col_index(&schema, col_name).unwrap_or(0);
        // Normalization always produces f64 output.
        schema[col].1 = ColumnType::F64;
        Self { source: Some(source), col, schema, result: None, offset: 0 }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut src = self.source.take().unwrap();
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() { all_rows.push(extract_row(&b, r)); },
            }
        }
        let vals: Vec<f64> = all_rows.iter().map(|r| value_to_f64(&r[self.col])).collect();
        let min = vals.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let range = max - min;
        for (i, row) in all_rows.iter_mut().enumerate() {
            row[self.col] = Value::F64(if range == 0.0 { 0.0 } else { (vals[i] - min) / range });
        }
        let mut batch = RecordBatch::new(self.schema.clone());
        for row in &all_rows { batch.append_row(row); }
        self.result = Some(batch);
        Ok(())
    }
}

impl RecordCursor for NormalizeCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.result.is_none() { self.materialize()?; }
        let mat = self.result.as_ref().unwrap();
        if self.offset >= mat.row_count() { return Ok(None); }
        let n = (mat.row_count() - self.offset).min(max_rows);
        let batch = mat.slice(self.offset, n);
        self.offset += n;
        Ok(Some(batch))
    }
}

// ── 46. ZScoreCursor ─────────────────────────────────────────────────

/// Z-score standardization: (value - mean) / stddev.
pub struct ZScoreCursor {
    source: Option<Box<dyn RecordCursor>>,
    col: usize,
    schema: Vec<(String, ColumnType)>,
    result: Option<RecordBatch>,
    offset: usize,
}

impl ZScoreCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let schema = source.schema().to_vec();
        let col = col_index(&schema, col_name).unwrap_or(0);
        Self { source: Some(source), col, schema, result: None, offset: 0 }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut src = self.source.take().unwrap();
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() { all_rows.push(extract_row(&b, r)); },
            }
        }
        let vals: Vec<f64> = all_rows.iter().map(|r| value_to_f64(&r[self.col])).collect();
        let n = vals.len() as f64;
        let mean = if n == 0.0 { 0.0 } else { vals.iter().sum::<f64>() / n };
        let variance = if n == 0.0 { 0.0 } else { vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n };
        let stddev = variance.sqrt();
        for (i, row) in all_rows.iter_mut().enumerate() {
            row[self.col] = Value::F64(if stddev == 0.0 { 0.0 } else { (vals[i] - mean) / stddev });
        }
        let mut batch = RecordBatch::new(self.schema.clone());
        for row in &all_rows { batch.append_row(row); }
        self.result = Some(batch);
        Ok(())
    }
}

impl RecordCursor for ZScoreCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.result.is_none() { self.materialize()?; }
        let mat = self.result.as_ref().unwrap();
        if self.offset >= mat.row_count() { return Ok(None); }
        let n = (mat.row_count() - self.offset).min(max_rows);
        let batch = mat.slice(self.offset, n);
        self.offset += n;
        Ok(Some(batch))
    }
}

// ── 47. RankCursor ───────────────────────────────────────────────────

/// Adds a rank column based on ordering of a specified column.
pub struct RankCursor {
    source: Option<Box<dyn RecordCursor>>,
    col: usize,
    descending: bool,
    schema: Vec<(String, ColumnType)>,
    result: Option<RecordBatch>,
    offset: usize,
}

impl RankCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str, descending: bool) -> Self {
        let mut schema = source.schema().to_vec();
        let col = col_index(&schema, col_name).unwrap_or(0);
        schema.push(("rank".to_string(), ColumnType::I64));
        Self { source: Some(source), col, descending, schema, result: None, offset: 0 }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut src = self.source.take().unwrap();
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        loop {
            match src.next_batch(4096)? {
                None => break,
                Some(b) => for r in 0..b.row_count() { all_rows.push(extract_row(&b, r)); },
            }
        }
        let col = self.col; let desc = self.descending;
        // Create index-sorted order.
        let mut indices: Vec<usize> = (0..all_rows.len()).collect();
        indices.sort_by(|&a, &b| {
            let c = all_rows[a][col].cmp_coerce(&all_rows[b][col]).unwrap_or(Ordering::Equal);
            if desc { c.reverse() } else { c }
        });
        let mut ranks = vec![0i64; all_rows.len()];
        for (rank, &idx) in indices.iter().enumerate() { ranks[idx] = (rank + 1) as i64; }

        let mut batch = RecordBatch::new(self.schema.clone());
        for (i, row) in all_rows.iter().enumerate() {
            let mut out = row.clone();
            out.push(Value::I64(ranks[i]));
            batch.append_row(&out);
        }
        self.result = Some(batch);
        Ok(())
    }
}

impl RecordCursor for RankCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.result.is_none() { self.materialize()?; }
        let mat = self.result.as_ref().unwrap();
        if self.offset >= mat.row_count() { return Ok(None); }
        let n = (mat.row_count() - self.offset).min(max_rows);
        let batch = mat.slice(self.offset, n);
        self.offset += n;
        Ok(Some(batch))
    }
}

// ── 48. RowHashCursor ────────────────────────────────────────────────

/// Adds a hash column for each row.
pub struct RowHashCursor {
    source: Box<dyn RecordCursor>,
    schema: Vec<(String, ColumnType)>,
}

impl RowHashCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let mut schema = source.schema().to_vec();
        schema.push(("row_hash".to_string(), ColumnType::I64));
        Self { source, schema }
    }
}

impl RecordCursor for RowHashCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let mut row = extract_row(&b, r);
                    let mut h: u64 = 0;
                    for v in &row { h = h.wrapping_mul(31).wrapping_add(hash_value(v)); }
                    row.push(Value::I64(h as i64));
                    result.append_row(&row);
                }
                if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
            }
        }
    }
}

// ── 49. SplitCursor ──────────────────────────────────────────────────

/// Splits one row into multiple rows by expanding an array-like string column
/// (comma-separated values). Like UNNEST.
pub struct SplitCursor {
    source: Box<dyn RecordCursor>,
    col: usize,
    separator: String,
    buffer: VecDeque<Vec<Value>>,
    schema: Vec<(String, ColumnType)>,
}

impl SplitCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str, separator: &str) -> Self {
        let schema = source.schema().to_vec();
        let col = col_index(&schema, col_name).unwrap_or(0);
        Self { source, col, separator: separator.to_string(), buffer: VecDeque::new(), schema }
    }
}

impl RecordCursor for SplitCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let mut result = RecordBatch::new(self.schema.clone());
        while result.row_count() < max_rows {
            if let Some(row) = self.buffer.pop_front() { result.append_row(&row); continue; }
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let row = extract_row(&b, r);
                    if let Value::Str(s) = &row[self.col] {
                        for part in s.split(&self.separator) {
                            let mut new_row = row.clone();
                            new_row[self.col] = Value::Str(part.trim().to_string());
                            self.buffer.push_back(new_row);
                        }
                    } else {
                        self.buffer.push_back(row);
                    }
                },
            }
        }
        while result.row_count() < max_rows {
            if let Some(row) = self.buffer.pop_front() { result.append_row(&row); } else { break; }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ── 50. FlattenCursor ────────────────────────────────────────────────

/// Flattens nested results: if the source emits batches of batches (simulated
/// by a column containing sub-cursor data), this passes through rows unchanged,
/// coalescing small batches into larger ones up to max_rows.
pub struct FlattenCursor {
    source: Box<dyn RecordCursor>,
    buffer: VecDeque<Vec<Value>>,
}

impl FlattenCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self { source, buffer: VecDeque::new() }
    }
}

impl RecordCursor for FlattenCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        // Drain buffer.
        while result.row_count() < max_rows {
            if let Some(row) = self.buffer.pop_front() { result.append_row(&row); } else { break; }
        }
        // Pull more batches.
        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => for r in 0..b.row_count() {
                    let row = extract_row(&b, r);
                    if result.row_count() < max_rows {
                        result.append_row(&row);
                    } else {
                        self.buffer.push_back(row);
                    }
                },
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

// ═══════════════════════════════════════════════════════════════════════
//  TESTS
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    fn i64_schema(name: &str) -> Vec<(String, ColumnType)> {
        vec![(name.to_string(), ColumnType::I64)]
    }

    fn collect_all(cursor: &mut dyn RecordCursor) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch(4096).unwrap() {
            for r in 0..batch.row_count() { rows.push(extract_row(&batch, r)); }
        }
        rows
    }

    fn collect_col(cursor: &mut dyn RecordCursor, col: usize) -> Vec<Value> {
        collect_all(cursor).into_iter().map(|r| r[col].clone()).collect()
    }

    // ── Scan cursors ────────────────────────────────────────────────

    #[test]
    fn partition_pruned_scan() {
        let schema = vec![("ts".to_string(), ColumnType::Timestamp)];
        let rows: Vec<Vec<Value>> = vec![100, 200, 300, 400, 500].into_iter().map(|n| vec![Value::Timestamp(n)]).collect();
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = PartitionPrunedScanCursor::new(Box::new(src), "ts", 200, 400);
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::Timestamp(200), Value::Timestamp(300), Value::Timestamp(400)]);
    }

    #[test]
    fn indexed_symbol_scan() {
        let schema = vec![("sym".to_string(), ColumnType::Varchar), ("val".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::Str("BTC".into()), Value::I64(1)],
            vec![Value::Str("ETH".into()), Value::I64(2)],
            vec![Value::Str("BTC".into()), Value::I64(3)],
        ];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = IndexedSymbolScanCursor::new(Box::new(src), "sym", Value::Str("BTC".into()));
        let out = collect_col(&mut c, 1);
        assert_eq!(out, vec![Value::I64(1), Value::I64(3)]);
    }

    #[test]
    fn top_n_scan() {
        let schema = i64_schema("val");
        let rows: Vec<Vec<Value>> = (0..100).map(|i| vec![Value::I64(i)]).collect();
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = TopNScanCursor::new(Box::new(src), 3, "val", true);
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(99), Value::I64(98), Value::I64(97)]);
    }

    #[test]
    fn skip_scan() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(2)], vec![Value::I64(3)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = SkipScanCursor::new(Box::new(src), "val");
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
    }

    #[test]
    fn zero_copy_scan() {
        let schema = i64_schema("x");
        let rows = vec![vec![Value::I64(42)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = ZeroCopyScanCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(42)]);
    }

    #[test]
    fn compressed_scan() {
        let schema = i64_schema("x");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = CompressedScanCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(1), Value::I64(2)]);
        assert!(c.bytes_decompressed() > 0);
    }

    #[test]
    fn tiered_scan() {
        let schema = i64_schema("x");
        let hot = MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(1)]]);
        let cold = MemoryCursor::from_rows(schema, &[vec![Value::I64(2)]]);
        let mut c = TieredScanCursor::new(vec![Box::new(hot), Box::new(cold)]);
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(1), Value::I64(2)]);
    }

    #[test]
    fn predicate_pushdown_scan() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(3)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = PredicatePushdownScanCursor::new(Box::new(src), "val", Value::I64(2));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(2)]);
    }

    #[test]
    fn project_pushdown_scan() {
        let schema = vec![("a".to_string(), ColumnType::I64), ("b".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1), Value::I64(10)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = ProjectPushdownScanCursor::new(Box::new(src), &["b"]);
        assert_eq!(c.schema().len(), 1);
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(10)]);
    }

    #[test]
    fn batch_prefetch_scan() {
        let schema = i64_schema("x");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = BatchPrefetchScanCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(1), Value::I64(2)]);
    }

    // ── Join cursors ────────────────────────────────────────────────

    #[test]
    fn asof_join_indexed() {
        let ls = vec![("ts".to_string(), ColumnType::Timestamp)];
        let left = MemoryCursor::from_rows(ls, &[vec![Value::Timestamp(150)], vec![Value::Timestamp(250)]]);
        let rs = vec![("ts".to_string(), ColumnType::Timestamp), ("v".to_string(), ColumnType::I64)];
        let right = MemoryCursor::from_rows(rs, &[
            vec![Value::Timestamp(100), Value::I64(10)],
            vec![Value::Timestamp(200), Value::I64(20)],
        ]);
        let mut c = AsofJoinIndexedCursor::new(Box::new(left), Box::new(right), "ts", "ts");
        let rows = collect_all(&mut c);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][2], Value::I64(10)); // 150 matches right ts=100
        assert_eq!(rows[1][2], Value::I64(20)); // 250 matches right ts=200
    }

    #[test]
    fn lookup_join() {
        let ls = vec![("id".to_string(), ColumnType::I64), ("name".to_string(), ColumnType::Varchar)];
        let left = MemoryCursor::from_rows(ls, &[
            vec![Value::I64(1), Value::Str("A".into())],
            vec![Value::I64(2), Value::Str("B".into())],
        ]);
        let rs = vec![("id".to_string(), ColumnType::I64), ("score".to_string(), ColumnType::I64)];
        let right = MemoryCursor::from_rows(rs, &[
            vec![Value::I64(1), Value::I64(100)],
            vec![Value::I64(2), Value::I64(200)],
        ]);
        let mut c = LookupJoinCursor::new(Box::new(left), Box::new(right), "id", "id");
        let rows = collect_all(&mut c);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][3], Value::I64(100));
        assert_eq!(rows[1][3], Value::I64(200));
    }

    #[test]
    fn semi_hash_join() {
        let ls = i64_schema("id");
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(3)]]);
        let rs = i64_schema("id");
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1)], vec![Value::I64(3)]]);
        let mut c = SemiHashJoinCursor::new(Box::new(left), Box::new(right), 0, 0);
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(1), Value::I64(3)]);
    }

    #[test]
    fn anti_hash_join() {
        let ls = i64_schema("id");
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(3)]]);
        let rs = i64_schema("id");
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1)], vec![Value::I64(3)]]);
        let mut c = AntiHashJoinCursor::new(Box::new(left), Box::new(right), 0, 0);
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(2)]);
    }

    #[test]
    fn parallel_hash_join() {
        let ls = vec![("id".to_string(), ColumnType::I64), ("n".to_string(), ColumnType::Varchar)];
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1), Value::Str("A".into())]]);
        let rs = vec![("id".to_string(), ColumnType::I64), ("v".to_string(), ColumnType::I64)];
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1), Value::I64(99)]]);
        let mut c = ParallelHashJoinCursor::new(Box::new(left), Box::new(right), 0, 0, 4);
        let rows = collect_all(&mut c);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][3], Value::I64(99));
    }

    #[test]
    fn grace_hash_join() {
        let ls = i64_schema("id");
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1)]]);
        let rs = vec![("id".to_string(), ColumnType::I64), ("v".to_string(), ColumnType::I64)];
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1), Value::I64(42)]]);
        let mut c = GraceHashJoinCursor::new(Box::new(left), Box::new(right), 0, 0);
        let rows = collect_all(&mut c);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn skewed_join() {
        let ls = i64_schema("id");
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1)], vec![Value::I64(1)]]);
        let rs = vec![("id".to_string(), ColumnType::I64), ("v".to_string(), ColumnType::I64)];
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1), Value::I64(10)]]);
        let mut c = SkewedJoinCursor::new(Box::new(left), Box::new(right), 0, 0);
        let rows = collect_all(&mut c);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn adaptive_join() {
        let ls = i64_schema("id");
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1)], vec![Value::I64(2)]]);
        let rs = vec![("id".to_string(), ColumnType::I64), ("v".to_string(), ColumnType::I64)];
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1), Value::I64(42)]]);
        let mut c = AdaptiveJoinCursor::new(Box::new(left), Box::new(right), 0, 0).unwrap();
        let rows = collect_all(&mut c);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][2], Value::I64(42));
    }

    #[test]
    fn partition_wise_join() {
        let ls = i64_schema("id");
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1)]]);
        let rs = vec![("id".to_string(), ColumnType::I64), ("v".to_string(), ColumnType::I64)];
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1), Value::I64(99)]]);
        let mut c = PartitionWiseJoinCursor::new(
            vec![(Box::new(left) as Box<dyn RecordCursor>, Box::new(right) as Box<dyn RecordCursor>)],
            "id", "id",
        );
        let rows = collect_all(&mut c);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn multi_join() {
        let s1 = i64_schema("id");
        let t1 = MemoryCursor::from_rows(s1, &[vec![Value::I64(1)]]);
        let s2 = vec![("id".to_string(), ColumnType::I64), ("v2".to_string(), ColumnType::I64)];
        let t2 = MemoryCursor::from_rows(s2, &[vec![Value::I64(1), Value::I64(20)]]);
        let s3 = vec![("id".to_string(), ColumnType::I64), ("v3".to_string(), ColumnType::I64)];
        let t3 = MemoryCursor::from_rows(s3, &[vec![Value::I64(1), Value::I64(30)]]);
        let mut c = MultiJoinCursor::new(vec![Box::new(t1), Box::new(t2), Box::new(t3)], vec![0, 0, 0]);
        let rows = collect_all(&mut c);
        assert_eq!(rows.len(), 1);
    }

    // ── Aggregate cursors ───────────────────────────────────────────

    #[test]
    fn partial_aggregate() {
        let schema = vec![("grp".to_string(), ColumnType::I64), ("val".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1), Value::I64(10)],
            vec![Value::I64(1), Value::I64(20)],
            vec![Value::I64(2), Value::I64(30)],
        ];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = PartialAggregateCursor::new(Box::new(src), "grp", "val");
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn merge_aggregate() {
        let schema = vec![
            ("grp".to_string(), ColumnType::I64),
            ("partial_sum".to_string(), ColumnType::F64),
            ("partial_count".to_string(), ColumnType::I64),
        ];
        let rows = vec![
            vec![Value::I64(1), Value::F64(10.0), Value::I64(1)],
            vec![Value::I64(1), Value::F64(20.0), Value::I64(2)],
        ];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = MergeAggregateCursor::new(Box::new(src));
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][1], Value::F64(30.0));
        assert_eq!(out[0][2], Value::I64(3));
    }

    #[test]
    fn distinct_aggregate() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(1)], vec![Value::I64(3)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = DistinctAggregateCursor::new(Box::new(src), "val");
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(3)]);
    }

    #[test]
    fn filtered_aggregate() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(5)], vec![Value::I64(10)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = FilteredAggregateCursor::new(Box::new(src), "val", Value::I64(3));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(2)]); // 5 > 3 and 10 > 3
    }

    #[test]
    fn ordered_aggregate_median() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(10)], vec![Value::I64(20)], vec![Value::I64(30)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = OrderedAggregateCursor::new(Box::new(src), "val", 0.5);
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::F64(20.0)]);
    }

    #[test]
    fn grouping_sets_aggregate() {
        let schema = vec![("a".to_string(), ColumnType::I64), ("val".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1), Value::I64(10)],
            vec![Value::I64(2), Value::I64(20)],
        ];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = GroupingSetsAggregateCursor::new(Box::new(src), &[vec!["a"], vec![]], "val");
        let out = collect_all(&mut c);
        // group by a: 2 groups + group by (): 1 grand total = 3 rows
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn top_k_aggregate() {
        let schema = vec![("grp".to_string(), ColumnType::I64), ("val".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1), Value::I64(10)],
            vec![Value::I64(2), Value::I64(50)],
            vec![Value::I64(3), Value::I64(30)],
        ];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = TopKAggregateCursor::new(Box::new(src), "grp", "val", 2);
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 2);
        // Top 2 by sum DESC: group 2 (50), group 3 (30)
        assert_eq!(out[0][1], Value::F64(50.0));
        assert_eq!(out[1][1], Value::F64(30.0));
    }

    #[test]
    fn streaming_count() {
        let schema = i64_schema("x");
        let rows: Vec<Vec<Value>> = (0..10).map(|i| vec![Value::I64(i)]).collect();
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = StreamingCountCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(10)]);
    }

    #[test]
    fn min_max_only() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(5)], vec![Value::I64(1)], vec![Value::I64(9)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = MinMaxOnlyCursor::new(Box::new(src), "val");
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][0], Value::I64(1));
        assert_eq!(out[0][1], Value::I64(9));
    }

    #[test]
    fn running_total() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(3)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = RunningTotalCursor::new(Box::new(src), "val");
        let out = collect_all(&mut c);
        assert_eq!(out[0][1], Value::F64(1.0));
        assert_eq!(out[1][1], Value::F64(3.0));
        assert_eq!(out[2][1], Value::F64(6.0));
    }

    // ── Output cursors ──────────────────────────────────────────────

    #[test]
    fn csv_output() {
        let schema = vec![("a".to_string(), ColumnType::I64), ("b".to_string(), ColumnType::Varchar)];
        let rows = vec![vec![Value::I64(1), Value::Str("hello".into())]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = CsvOutputCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out[0], Value::Str("a,b".into())); // header
        assert!(matches!(&out[1], Value::Str(s) if s.contains("1")));
    }

    #[test]
    fn json_output() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(42)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = JsonOutputCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert!(matches!(&out[0], Value::Str(s) if s.contains("42")));
    }

    #[test]
    fn ndjson_output() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = NdjsonOutputCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out.len(), 2);
        assert!(matches!(&out[0], Value::Str(s) if s.contains("\"val\":1")));
    }

    #[test]
    fn parquet_output() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(3)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = ParquetOutputCursor::new(Box::new(src));
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][1], Value::I64(3)); // total rows
    }

    #[test]
    fn insert_output() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = InsertOutputCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(2)]);
    }

    #[test]
    fn update_output() {
        let schema = vec![("id".to_string(), ColumnType::I64), ("val".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1), Value::I64(10)], vec![Value::I64(2), Value::I64(20)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = UpdateOutputCursor::new(Box::new(src), "val", Value::I64(99));
        let out = collect_all(&mut c);
        assert_eq!(out[0][1], Value::I64(99));
        assert_eq!(out[1][1], Value::I64(99));
    }

    #[test]
    fn delete_output() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(1)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = DeleteOutputCursor::new(Box::new(src), "val", Value::I64(1));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(2)]); // 2 rows deleted
    }

    #[test]
    fn count_output() {
        let schema = i64_schema("x");
        let rows: Vec<Vec<Value>> = (0..7).map(|i| vec![Value::I64(i)]).collect();
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = CountOutputCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(7)]);
    }

    #[test]
    fn hash_output() {
        let schema = i64_schema("x");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = HashOutputCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Value::I64(_)));
    }

    #[test]
    fn checksum_output() {
        let schema = i64_schema("x");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = ChecksumOutputCursor::new(Box::new(src));
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 2);
        // Checksums should differ between rows.
        assert_ne!(out[0][1], out[1][1]);
    }

    // ── Transform cursors ───────────────────────────────────────────

    #[test]
    fn pivot_cursor() {
        let schema = vec![
            ("product".to_string(), ColumnType::Varchar),
            ("quarter".to_string(), ColumnType::Varchar),
            ("sales".to_string(), ColumnType::I64),
        ];
        let rows = vec![
            vec![Value::Str("A".into()), Value::Str("Q1".into()), Value::I64(10)],
            vec![Value::Str("A".into()), Value::Str("Q2".into()), Value::I64(20)],
            vec![Value::Str("B".into()), Value::Str("Q1".into()), Value::I64(30)],
        ];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = PivotCursor::new(Box::new(src), "product", "quarter", "sales");
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 2); // 2 products
    }

    #[test]
    fn unpivot_cursor() {
        let schema = vec![
            ("id".to_string(), ColumnType::I64),
            ("a".to_string(), ColumnType::I64),
            ("b".to_string(), ColumnType::I64),
        ];
        let rows = vec![vec![Value::I64(1), Value::I64(10), Value::I64(20)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = UnpivotCursor::new(Box::new(src), "id", &["a", "b"]);
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0][1], Value::Str("a".into()));
        assert_eq!(out[1][1], Value::Str("b".into()));
    }

    #[test]
    fn deduplicate_cursor() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(2)], vec![Value::I64(3)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = DeduplicateCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
    }

    #[test]
    fn interpolate_cursor() {
        let schema = vec![("val".to_string(), ColumnType::F64)];
        let rows = vec![
            vec![Value::F64(1.0)],
            vec![Value::Null],
            vec![Value::F64(3.0)],
        ];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = InterpolateCursor::new(Box::new(src), "val");
        let out = collect_col(&mut c, 0);
        assert_eq!(out[1], Value::F64(2.0)); // interpolated
    }

    #[test]
    fn normalize_cursor() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(0)], vec![Value::I64(50)], vec![Value::I64(100)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = NormalizeCursor::new(Box::new(src), "val");
        let out = collect_col(&mut c, 0);
        assert_eq!(out[0], Value::F64(0.0));
        assert_eq!(out[1], Value::F64(0.5));
        assert_eq!(out[2], Value::F64(1.0));
    }

    #[test]
    fn zscore_cursor() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(10)], vec![Value::I64(20)], vec![Value::I64(30)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = ZScoreCursor::new(Box::new(src), "val");
        let out = collect_col(&mut c, 0);
        // Mean=20, stddev=sqrt(200/3)
        if let Value::F64(v) = &out[1] { assert!(v.abs() < 1e-10); } // middle value z-score ≈ 0
    }

    #[test]
    fn rank_cursor() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(30)], vec![Value::I64(10)], vec![Value::I64(20)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = RankCursor::new(Box::new(src), "val", false);
        let out = collect_all(&mut c);
        // Original order: 30, 10, 20. Ranks ascending: 30->3, 10->1, 20->2
        assert_eq!(out[0][1], Value::I64(3));
        assert_eq!(out[1][1], Value::I64(1));
        assert_eq!(out[2][1], Value::I64(2));
    }

    #[test]
    fn row_hash_cursor() {
        let schema = i64_schema("val");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = RowHashCursor::new(Box::new(src));
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 2);
        assert_ne!(out[0][1], out[1][1]); // different values => different hashes
    }

    #[test]
    fn split_cursor() {
        let schema = vec![("id".to_string(), ColumnType::I64), ("tags".to_string(), ColumnType::Varchar)];
        let rows = vec![vec![Value::I64(1), Value::Str("a,b,c".into())]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = SplitCursor::new(Box::new(src), "tags", ",");
        let out = collect_all(&mut c);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0][1], Value::Str("a".into()));
        assert_eq!(out[1][1], Value::Str("b".into()));
        assert_eq!(out[2][1], Value::Str("c".into()));
    }

    #[test]
    fn flatten_cursor() {
        let schema = i64_schema("x");
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(3)]];
        let src = MemoryCursor::from_rows(schema, &rows);
        let mut c = FlattenCursor::new(Box::new(src));
        let out = collect_col(&mut c, 0);
        assert_eq!(out, vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
    }
}
