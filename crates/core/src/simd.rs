//! SIMD-accelerated operations on column data.
//!
//! Provides vectorised sum, min, max, count, and filter operations for
//! `f64` and `i64` slices.  The implementation uses chunked processing
//! (4-wide for f64, 4-wide for i64) that LLVM auto-vectorises on x86-64
//! and aarch64.  A scalar tail loop handles remaining elements.

/// Sum all `f64` values in a slice using a 4-wide accumulator.
/// Falls back to a scalar loop for the tail.
pub fn sum_f64(data: &[f64]) -> f64 {
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    let mut acc = [0.0f64; 4];
    for chunk in chunks {
        acc[0] += chunk[0];
        acc[1] += chunk[1];
        acc[2] += chunk[2];
        acc[3] += chunk[3];
    }

    let mut total = acc[0] + acc[1] + acc[2] + acc[3];
    for &v in remainder {
        total += v;
    }
    total
}

/// Sum all `i64` values in a slice using a 4-wide accumulator.
pub fn sum_i64(data: &[i64]) -> i64 {
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    let mut acc = [0i64; 4];
    for chunk in chunks {
        acc[0] = acc[0].wrapping_add(chunk[0]);
        acc[1] = acc[1].wrapping_add(chunk[1]);
        acc[2] = acc[2].wrapping_add(chunk[2]);
        acc[3] = acc[3].wrapping_add(chunk[3]);
    }

    let mut total = acc[0]
        .wrapping_add(acc[1])
        .wrapping_add(acc[2])
        .wrapping_add(acc[3]);
    for &v in remainder {
        total = total.wrapping_add(v);
    }
    total
}

/// Find the minimum `f64` value in a slice.
///
/// Returns `f64::INFINITY` for an empty slice (the identity element for min).
/// NaN values are treated as larger than any finite value (skipped).
pub fn min_f64(data: &[f64]) -> f64 {
    if data.is_empty() {
        return f64::INFINITY;
    }

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    let mut mins = [f64::INFINITY; 4];
    for chunk in chunks {
        for (i, &val) in chunk.iter().enumerate() {
            // Use a branchless comparison that skips NaN.
            if val < mins[i] {
                mins[i] = val;
            }
        }
    }

    let mut result = mins[0];
    for item in &mins[1..] {
        if *item < result {
            result = *item;
        }
    }
    for &v in remainder {
        if v < result {
            result = v;
        }
    }
    result
}

/// Find the maximum `f64` value in a slice.
///
/// Returns `f64::NEG_INFINITY` for an empty slice.
/// NaN values are skipped.
pub fn max_f64(data: &[f64]) -> f64 {
    if data.is_empty() {
        return f64::NEG_INFINITY;
    }

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    let mut maxs = [f64::NEG_INFINITY; 4];
    for chunk in chunks {
        for (i, &val) in chunk.iter().enumerate() {
            if val > maxs[i] {
                maxs[i] = val;
            }
        }
    }

    let mut result = maxs[0];
    for item in &maxs[1..] {
        if *item > result {
            result = *item;
        }
    }
    for &v in remainder {
        if v > result {
            result = v;
        }
    }
    result
}

/// Find the minimum `i64` value in a slice.
///
/// Returns `i64::MAX` for an empty slice.
pub fn min_i64(data: &[i64]) -> i64 {
    if data.is_empty() {
        return i64::MAX;
    }

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    let mut mins = [i64::MAX; 4];
    for chunk in chunks {
        for (i, &val) in chunk.iter().enumerate() {
            if val < mins[i] {
                mins[i] = val;
            }
        }
    }

    let mut result = mins[0];
    for item in &mins[1..] {
        if *item < result {
            result = *item;
        }
    }
    for &v in remainder {
        if v < result {
            result = v;
        }
    }
    result
}

/// Find the maximum `i64` value in a slice.
///
/// Returns `i64::MIN` for an empty slice.
pub fn max_i64(data: &[i64]) -> i64 {
    if data.is_empty() {
        return i64::MIN;
    }

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    let mut maxs = [i64::MIN; 4];
    for chunk in chunks {
        for (i, &val) in chunk.iter().enumerate() {
            if val > maxs[i] {
                maxs[i] = val;
            }
        }
    }

    let mut result = maxs[0];
    for item in &maxs[1..] {
        if *item > result {
            result = *item;
        }
    }
    for &v in remainder {
        if v > result {
            result = v;
        }
    }
    result
}

/// Count non-null `f64` values. A value is considered null if it is NaN.
pub fn count_non_null_f64(data: &[f64]) -> u64 {
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    let mut counts = [0u64; 4];
    for chunk in chunks {
        for i in 0..4 {
            if !chunk[i].is_nan() {
                counts[i] += 1;
            }
        }
    }

    let mut total = counts[0] + counts[1] + counts[2] + counts[3];
    for &v in remainder {
        if !v.is_nan() {
            total += 1;
        }
    }
    total
}

/// Return indices where `data[i] > threshold`.
pub fn filter_gt_f64(data: &[f64], threshold: f64) -> Vec<u32> {
    let mut result = Vec::new();
    let chunks = data.chunks_exact(4);
    let remainder_start = data.len() - chunks.remainder().len();
    let remainder = chunks.remainder();

    let mut base: u32 = 0;
    for chunk in chunks {
        for (j, &val) in chunk.iter().enumerate() {
            if val > threshold {
                result.push(base + j as u32);
            }
        }
        base += 4;
    }

    for (j, &v) in remainder.iter().enumerate() {
        if v > threshold {
            result.push(remainder_start as u32 + j as u32);
        }
    }

    result
}

/// Return indices where `data[i] == value`.
pub fn filter_eq_i64(data: &[i64], value: i64) -> Vec<u32> {
    let mut result = Vec::new();
    let chunks = data.chunks_exact(4);
    let remainder_start = data.len() - chunks.remainder().len();
    let remainder = chunks.remainder();

    let mut base: u32 = 0;
    for chunk in chunks {
        for (j, &val) in chunk.iter().enumerate() {
            if val == value {
                result.push(base + j as u32);
            }
        }
        base += 4;
    }

    for (j, &v) in remainder.iter().enumerate() {
        if v == value {
            result.push(remainder_start as u32 + j as u32);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── sum tests ──────────────────────────────────────────────────────

    #[test]
    fn sum_f64_matches_scalar() {
        let data: Vec<f64> = (0..1_000_000).map(|i| i as f64 * 0.001).collect();
        let scalar: f64 = data.iter().sum();
        let simd = sum_f64(&data);
        assert!(
            (simd - scalar).abs() < 1e-6,
            "simd={simd}, scalar={scalar}"
        );
    }

    #[test]
    fn sum_f64_empty() {
        assert_eq!(sum_f64(&[]), 0.0);
    }

    #[test]
    fn sum_f64_single() {
        assert_eq!(sum_f64(&[42.0]), 42.0);
    }

    #[test]
    fn sum_f64_three_elements() {
        assert_eq!(sum_f64(&[1.0, 2.0, 3.0]), 6.0);
    }

    #[test]
    fn sum_i64_matches_scalar() {
        let data: Vec<i64> = (0..1_000_000).collect();
        let scalar: i64 = data.iter().sum();
        let simd = sum_i64(&data);
        assert_eq!(simd, scalar);
    }

    #[test]
    fn sum_i64_empty() {
        assert_eq!(sum_i64(&[]), 0);
    }

    // ── min/max tests ──────────────────────────────────────────────────

    #[test]
    fn min_f64_basic() {
        let data = [3.0, 1.0, 4.0, 1.5, 9.2, 6.5, 3.5, 8.9, 7.9];
        assert_eq!(min_f64(&data), 1.0);
    }

    #[test]
    fn max_f64_basic() {
        let data = [3.0, 1.0, 4.0, 1.5, 9.2, 6.5, 3.5, 8.9, 7.9];
        assert_eq!(max_f64(&data), 9.2);
    }

    #[test]
    fn min_f64_empty() {
        assert_eq!(min_f64(&[]), f64::INFINITY);
    }

    #[test]
    fn max_f64_empty() {
        assert_eq!(max_f64(&[]), f64::NEG_INFINITY);
    }

    #[test]
    fn min_f64_single() {
        assert_eq!(min_f64(&[42.0]), 42.0);
    }

    #[test]
    fn max_f64_single() {
        assert_eq!(max_f64(&[42.0]), 42.0);
    }

    #[test]
    fn min_f64_with_nan() {
        let data = [f64::NAN, 5.0, 3.0, f64::NAN, 1.0];
        assert_eq!(min_f64(&data), 1.0);
    }

    #[test]
    fn max_f64_with_nan() {
        let data = [f64::NAN, 5.0, 3.0, f64::NAN, 1.0];
        assert_eq!(max_f64(&data), 5.0);
    }

    #[test]
    fn min_i64_basic() {
        let data = [5, 3, 8, 1, 9, 2];
        assert_eq!(min_i64(&data), 1);
    }

    #[test]
    fn max_i64_basic() {
        let data = [5, 3, 8, 1, 9, 2];
        assert_eq!(max_i64(&data), 9);
    }

    #[test]
    fn min_i64_empty() {
        assert_eq!(min_i64(&[]), i64::MAX);
    }

    #[test]
    fn max_i64_empty() {
        assert_eq!(max_i64(&[]), i64::MIN);
    }

    #[test]
    fn min_max_f64_large() {
        let data: Vec<f64> = (0..1_000_000).map(|i| i as f64).collect();
        assert_eq!(min_f64(&data), 0.0);
        assert_eq!(max_f64(&data), 999_999.0);
    }

    #[test]
    fn min_max_i64_large() {
        let data: Vec<i64> = (0..1_000_000).collect();
        assert_eq!(min_i64(&data), 0);
        assert_eq!(max_i64(&data), 999_999);
    }

    // ── count tests ────────────────────────────────────────────────────

    #[test]
    fn count_non_null_f64_basic() {
        let data = [1.0, f64::NAN, 3.0, f64::NAN, 5.0];
        assert_eq!(count_non_null_f64(&data), 3);
    }

    #[test]
    fn count_non_null_f64_all_nan() {
        let data = [f64::NAN; 8];
        assert_eq!(count_non_null_f64(&data), 0);
    }

    #[test]
    fn count_non_null_f64_none_nan() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(count_non_null_f64(&data), 5);
    }

    #[test]
    fn count_non_null_f64_empty() {
        assert_eq!(count_non_null_f64(&[]), 0);
    }

    // ── filter tests ───────────────────────────────────────────────────

    #[test]
    fn filter_gt_f64_basic() {
        let data = [1.0, 5.0, 3.0, 7.0, 2.0, 8.0];
        let result = filter_gt_f64(&data, 4.0);
        assert_eq!(result, vec![1, 3, 5]);
    }

    #[test]
    fn filter_gt_f64_none() {
        let data = [1.0, 2.0, 3.0];
        let result = filter_gt_f64(&data, 10.0);
        assert!(result.is_empty());
    }

    #[test]
    fn filter_gt_f64_all() {
        let data = [5.0, 6.0, 7.0];
        let result = filter_gt_f64(&data, 0.0);
        assert_eq!(result, vec![0, 1, 2]);
    }

    #[test]
    fn filter_eq_i64_basic() {
        let data = [1, 2, 3, 2, 4, 2];
        let result = filter_eq_i64(&data, 2);
        assert_eq!(result, vec![1, 3, 5]);
    }

    #[test]
    fn filter_eq_i64_none() {
        let data = [1, 2, 3];
        let result = filter_eq_i64(&data, 99);
        assert!(result.is_empty());
    }

    // ── benchmark-style validation ─────────────────────────────────────

    #[test]
    fn simd_sum_vs_scalar_large_i64() {
        let n = 1_000_000;
        let data: Vec<i64> = (1..=n).collect();
        let expected = n * (n + 1) / 2;
        assert_eq!(sum_i64(&data), expected);
    }

    #[test]
    fn simd_sum_vs_scalar_large_f64() {
        let n = 100_000u64;
        let data: Vec<f64> = (1..=n).map(|i| i as f64).collect();
        let expected: f64 = (n * (n + 1) / 2) as f64;
        let result = sum_f64(&data);
        assert!(
            (result - expected).abs() / expected < 1e-10,
            "result={result}, expected={expected}"
        );
    }
}
