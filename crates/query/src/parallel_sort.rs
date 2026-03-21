//! Parallel merge sort for large query result sets.
//!
//! Splits data into chunks, sorts each chunk in parallel using rayon,
//! then merges the sorted chunks into a single sorted output.

use crate::plan::{OrderBy, Value};

/// Sort large datasets using parallel merge sort.
///
/// Splits `rows` into `parallelism` chunks, sorts each in parallel with
/// rayon, then merges. Falls back to sequential sort for small datasets.
pub fn parallel_sort(
    rows: &mut Vec<Vec<Value>>,
    order_by: &[OrderBy],
    parallelism: usize,
) {
    if rows.len() < 1024 || parallelism <= 1 || order_by.is_empty() {
        // For small datasets, sequential sort is faster (no thread overhead).
        sequential_sort(rows, order_by);
        return;
    }

    let chunk_size = rows.len().div_ceil(parallelism);
    let order_by_owned: Vec<OrderBy> = order_by.to_vec();

    // Drain rows into chunks.
    let mut chunks: Vec<Vec<Vec<Value>>> = Vec::with_capacity(parallelism);
    let all_rows = std::mem::take(rows);
    let mut iter = all_rows.into_iter();

    for _ in 0..parallelism {
        let chunk: Vec<Vec<Value>> = iter.by_ref().take(chunk_size).collect();
        if !chunk.is_empty() {
            chunks.push(chunk);
        }
    }

    // Sort each chunk in parallel.
    rayon::scope(|s| {
        for chunk in chunks.iter_mut() {
            let ob = &order_by_owned;
            s.spawn(move |_| {
                sequential_sort(chunk, ob);
            });
        }
    });

    // K-way merge of sorted chunks.
    *rows = k_way_merge(chunks, order_by);
}

/// Simple sequential sort using the order_by specification.
fn sequential_sort(rows: &mut [Vec<Value>], order_by: &[OrderBy]) {
    rows.sort_by(|a, b| compare_rows(a, b, order_by));
}

/// Compare two rows according to ORDER BY specifications.
///
/// Column names in `order_by` are matched by position — we use a
/// convention where the column index equals the column's position in the
/// row. For the optimizer's parallel sort, the caller must ensure rows
/// have a known column layout.
fn compare_rows(a: &[Value], b: &[Value], order_by: &[OrderBy]) -> std::cmp::Ordering {
    for ob in order_by {
        // Try to find column by name as an integer index, or search by name.
        // For now we use a simple heuristic: parse the column name as an index
        // if possible; otherwise treat as unknown (equal).
        let idx = column_index_for_sort(&ob.column);
        if let Some(idx) = idx {
            if idx >= a.len() || idx >= b.len() {
                continue;
            }
            let cmp = a[idx].cmp_coerce(&b[idx]);
            let ord = match cmp {
                Some(std::cmp::Ordering::Equal) => continue,
                Some(ord) => ord,
                None => continue,
            };
            return if ob.descending { ord.reverse() } else { ord };
        }
    }
    std::cmp::Ordering::Equal
}

/// Try to resolve a column name to an index for sorting.
///
/// This supports both numeric indices (used internally when the optimizer
/// sets up parallel sort with known column positions) and stores the
/// column name for later resolution.
fn column_index_for_sort(col: &str) -> Option<usize> {
    // If the column name starts with '#', treat the rest as an index.
    if let Some(rest) = col.strip_prefix('#') {
        rest.parse::<usize>().ok()
    } else {
        // For named columns, we embed the index in the column name when
        // setting up parallel sort. Try parsing directly as a number.
        col.parse::<usize>().ok()
    }
}

/// K-way merge of pre-sorted chunks.
fn k_way_merge(
    chunks: Vec<Vec<Vec<Value>>>,
    order_by: &[OrderBy],
) -> Vec<Vec<Value>> {
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    let mut result = Vec::with_capacity(total);

    // Track the current position in each chunk using indices.
    let mut positions: Vec<usize> = vec![0; chunks.len()];

    loop {
        // Find the chunk with the smallest next element.
        let mut best_chunk: Option<usize> = None;
        for i in 0..chunks.len() {
            if positions[i] >= chunks[i].len() {
                continue; // This chunk is exhausted.
            }
            match best_chunk {
                None => best_chunk = Some(i),
                Some(bi) => {
                    let current = &chunks[i][positions[i]];
                    let best = &chunks[bi][positions[bi]];
                    if compare_rows(current, best, order_by) == std::cmp::Ordering::Less {
                        best_chunk = Some(i);
                    }
                }
            }
        }

        match best_chunk {
            Some(idx) => {
                result.push(chunks[idx][positions[idx]].clone());
                positions[idx] += 1;
            }
            None => break, // All chunks exhausted.
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::Value;

    fn make_row(val: i64) -> Vec<Value> {
        vec![Value::I64(val)]
    }

    #[test]
    fn parallel_sort_matches_sequential() {
        let mut rows_par: Vec<Vec<Value>> = (0..1000)
            .rev()
            .map(|i| make_row(i))
            .collect();
        let mut rows_seq = rows_par.clone();

        let order_by = vec![OrderBy {
            column: "0".to_string(),
            descending: false,
        }];

        parallel_sort(&mut rows_par, &order_by, 4);
        sequential_sort(&mut rows_seq, &order_by);

        assert_eq!(rows_par, rows_seq);
    }

    #[test]
    fn parallel_sort_descending() {
        let mut rows: Vec<Vec<Value>> = (0..500).map(|i| make_row(i)).collect();
        let order_by = vec![OrderBy {
            column: "0".to_string(),
            descending: true,
        }];

        parallel_sort(&mut rows, &order_by, 4);

        for w in rows.windows(2) {
            match (&w[0][0], &w[1][0]) {
                (Value::I64(a), Value::I64(b)) => assert!(a >= b),
                _ => panic!("unexpected value type"),
            }
        }
    }

    #[test]
    fn parallel_sort_small_dataset_uses_sequential() {
        // Small datasets should still sort correctly (sequential fallback).
        let mut rows: Vec<Vec<Value>> = vec![make_row(3), make_row(1), make_row(2)];
        let order_by = vec![OrderBy {
            column: "0".to_string(),
            descending: false,
        }];

        parallel_sort(&mut rows, &order_by, 4);

        assert_eq!(rows[0][0], Value::I64(1));
        assert_eq!(rows[1][0], Value::I64(2));
        assert_eq!(rows[2][0], Value::I64(3));
    }

    #[test]
    fn parallel_sort_empty() {
        let mut rows: Vec<Vec<Value>> = vec![];
        let order_by = vec![OrderBy {
            column: "0".to_string(),
            descending: false,
        }];
        parallel_sort(&mut rows, &order_by, 4);
        assert!(rows.is_empty());
    }

    #[test]
    fn parallel_sort_single_element() {
        let mut rows = vec![make_row(42)];
        let order_by = vec![OrderBy {
            column: "0".to_string(),
            descending: false,
        }];
        parallel_sort(&mut rows, &order_by, 4);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(42));
    }

    #[test]
    fn parallel_sort_already_sorted() {
        let mut rows: Vec<Vec<Value>> = (0..2000).map(|i| make_row(i)).collect();
        let expected = rows.clone();
        let order_by = vec![OrderBy {
            column: "0".to_string(),
            descending: false,
        }];
        parallel_sort(&mut rows, &order_by, 4);
        assert_eq!(rows, expected);
    }

    #[test]
    fn k_way_merge_preserves_order() {
        let chunks = vec![
            vec![make_row(1), make_row(3), make_row(5)],
            vec![make_row(2), make_row(4), make_row(6)],
        ];
        let order_by = vec![OrderBy {
            column: "0".to_string(),
            descending: false,
        }];
        let merged = k_way_merge(chunks, &order_by);
        let expected: Vec<i64> = (1..=6).collect();
        let actual: Vec<i64> = merged
            .iter()
            .map(|r| match &r[0] {
                Value::I64(v) => *v,
                _ => panic!("unexpected"),
            })
            .collect();
        assert_eq!(actual, expected);
    }
}
