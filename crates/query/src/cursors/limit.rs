//! Limit/offset cursor.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Cursor that applies LIMIT and OFFSET semantics.
pub struct LimitCursor {
    source: Box<dyn RecordCursor>,
    limit: u64,
    offset: u64,
    rows_skipped: u64,
    rows_emitted: u64,
}

impl LimitCursor {
    pub fn new(source: Box<dyn RecordCursor>, limit: u64, offset: u64) -> Self {
        Self {
            source,
            limit,
            offset,
            rows_skipped: 0,
            rows_emitted: 0,
        }
    }
}

impl RecordCursor for LimitCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.rows_emitted >= self.limit {
            return Ok(None);
        }

        let remaining = (self.limit - self.rows_emitted) as usize;
        let want = max_rows.min(remaining);

        let schema: Vec<(String, ColumnType)> = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema.clone());

        loop {
            if result.row_count() >= want {
                break;
            }

            let batch = self.source.next_batch(want)?;
            match batch {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        // Skip rows for offset.
                        if self.rows_skipped < self.offset {
                            self.rows_skipped += 1;
                            continue;
                        }

                        if self.rows_emitted >= self.limit {
                            break;
                        }

                        let row: Vec<Value> =
                            (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                        result.append_row(&row);
                        self.rows_emitted += 1;

                        if result.row_count() >= want {
                            break;
                        }
                    }
                }
            }
        }

        if result.row_count() == 0 {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}
