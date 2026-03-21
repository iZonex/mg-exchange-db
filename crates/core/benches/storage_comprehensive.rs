//! Comprehensive storage benchmarks for ExchangeDB core layer.
//!
//! Covers: table writer throughput, table reader full scan, WAL write+commit,
//! partition pruning, bitmap index lookup vs full scan, LZ4 compression,
//! symbol map at scale, and concurrent read+write.

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::{FixedColumnReader, FixedColumnWriter};
use exchange_core::compression;
use exchange_core::index::bitmap::{BitmapIndexReader, BitmapIndexWriter};
use exchange_core::index::symbol_map::SymbolMap;
use exchange_core::simd;
use exchange_core::table::{ColumnValue, TableBuilder, TableMeta, TableWriter};
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use std::path::Path;
use tempfile::{tempdir, TempDir};

const HUNDRED_K: u64 = 100_000;
const MILLION: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a `trades` table (5 columns) and populate with `num_rows` rows
/// spanning `partition_days` daily partitions.
fn setup_trades_table(dir: &Path, num_rows: u64, partition_days: u64) -> TableMeta {
    let partition_by = if partition_days <= 1 {
        PartitionBy::None
    } else {
        PartitionBy::Day
    };

    let meta = TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("symbol", ColumnType::I32)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::F64)
        .column("side", ColumnType::I32)
        .timestamp("timestamp")
        .partition_by(partition_by)
        .build(dir)
        .expect("create table");

    let mut writer = TableWriter::open(dir, "trades").expect("open writer");

    let base_ts: i64 = 1_704_067_200_000_000_000;
    let total_span_ns = partition_days as i64 * 86_400_000_000_000;
    let step_ns = if num_rows > 1 {
        total_span_ns / num_rows as i64
    } else {
        1_000_000_000
    };

    let mut price = 50_000.0_f64;

    for i in 0..num_rows {
        let ts = Timestamp(base_ts + i as i64 * step_ns);
        let delta = ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64 * 0.5 - 2.5;
        price += delta;
        if price < 1.0 {
            price = 1.0;
        }

        writer
            .write_row(
                ts,
                &[
                    ColumnValue::I32((i % 1000) as i32),
                    ColumnValue::F64(price),
                    ColumnValue::F64(0.01 + (i % 10_000) as f64 * 0.01),
                    ColumnValue::I32((i % 2) as i32),
                ],
            )
            .expect("write_row");
    }
    writer.flush().expect("flush");
    meta
}

/// Create a table schema only (no data).
fn create_empty_trades(dir: &Path) -> TableMeta {
    TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("symbol", ColumnType::I32)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::F64)
        .column("side", ColumnType::I32)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(dir)
        .expect("create table")
}

// ---------------------------------------------------------------------------
// 1. Table writer throughput for different row counts
// ---------------------------------------------------------------------------

fn bench_table_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_writer_throughput");
    group.sample_size(10);

    for num_rows in [HUNDRED_K, 500_000, MILLION] {
        group.throughput(Throughput::Elements(num_rows));
        group.bench_with_input(
            BenchmarkId::new("rows", num_rows),
            &num_rows,
            |b, &rows| {
                b.iter_with_setup(
                    || {
                        let dir = TempDir::new().unwrap();
                        let _meta = create_empty_trades(dir.path());
                        dir
                    },
                    |dir| {
                        let mut writer =
                            TableWriter::open(dir.path(), "trades").unwrap();
                        let base_ts: i64 = 1_704_067_200_000_000_000;
                        let mut price = 50_000.0_f64;

                        for i in 0..rows {
                            let ts = Timestamp(base_ts + i as i64 * 1_000_000);
                            let delta = ((i.wrapping_mul(7).wrapping_add(3)) % 11)
                                as f64
                                * 0.5
                                - 2.5;
                            price += delta;

                            writer
                                .write_row(
                                    ts,
                                    &[
                                        ColumnValue::I32((i % 1000) as i32),
                                        ColumnValue::F64(black_box(price)),
                                        ColumnValue::F64(black_box(
                                            0.01 + (i % 10_000) as f64 * 0.01,
                                        )),
                                        ColumnValue::I32((i % 2) as i32),
                                    ],
                                )
                                .unwrap();
                        }
                        writer.flush().unwrap();
                    },
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Table reader full scan (via column readers)
// ---------------------------------------------------------------------------

fn bench_table_reader(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 1);

    // Open column readers for the single default partition.
    let table_dir = dir.path().join("trades").join("default");
    let price_reader =
        FixedColumnReader::open(&table_dir.join("price.d"), ColumnType::F64).unwrap();
    let volume_reader =
        FixedColumnReader::open(&table_dir.join("volume.d"), ColumnType::F64).unwrap();
    let row_count = price_reader.row_count();

    let mut group = c.benchmark_group("table_reader_scan");
    group.throughput(Throughput::Elements(row_count));
    group.sample_size(10);

    group.bench_function("1M_rows_price_volume", |b| {
        b.iter(|| {
            let mut sum_price = 0.0_f64;
            let mut sum_volume = 0.0_f64;
            for i in 0..row_count {
                sum_price += price_reader.read_f64(black_box(i));
                sum_volume += volume_reader.read_f64(black_box(i));
            }
            black_box((sum_price, sum_volume));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. WAL write + commit throughput
// ---------------------------------------------------------------------------

fn bench_wal_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_write_commit");
    group.throughput(Throughput::Elements(HUNDRED_K));
    group.sample_size(10);

    group.bench_function("100K_rows", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                // Create table with WAL support.
                let _meta = TableBuilder::new("wal_bench")
                    .column("timestamp", ColumnType::Timestamp)
                    .column("price", ColumnType::F64)
                    .column("volume", ColumnType::I64)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::Day)
                    .build(dir.path())
                    .unwrap();

                // Initialize _txn file.
                let table_dir = dir.path().join("wal_bench");
                let _txn =
                    exchange_core::txn::TxnFile::open(&table_dir).unwrap();

                dir
            },
            |dir| {
                let config = WalTableWriterConfig {
                    buffer_capacity: 10_000,
                    ..Default::default()
                };
                let mut writer =
                    WalTableWriter::open(dir.path(), "wal_bench", config).unwrap();

                let base_ts: i64 = 1_704_067_200_000_000_000;
                for i in 0..HUNDRED_K as i64 {
                    let ts_val = base_ts + i * 1_000_000;
                    let ts = Timestamp(ts_val);
                    writer
                        .write_row(
                            ts,
                            vec![
                                OwnedColumnValue::Timestamp(ts_val),
                                OwnedColumnValue::F64(50_000.0 + i as f64 * 0.01),
                                OwnedColumnValue::I64(i),
                            ],
                        )
                        .unwrap();
                }
                writer.commit().unwrap();
            },
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Partition pruning effectiveness
//    30 daily partitions: scan all vs scan 1 partition at the column level.
// ---------------------------------------------------------------------------

fn bench_partition_pruning(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 30);

    let table_dir = dir.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    let mut group = c.benchmark_group("partition_pruning");
    group.sample_size(10);

    // Full scan: read price column from all 30 partitions.
    let total_rows: u64 = partitions
        .iter()
        .map(|p| {
            FixedColumnReader::open(&p.join("price.d"), ColumnType::F64)
                .map(|r| r.row_count())
                .unwrap_or(0)
        })
        .sum();
    group.throughput(Throughput::Elements(total_rows));

    group.bench_function("full_scan_30_partitions", |b| {
        b.iter(|| {
            let mut total = 0.0_f64;
            for part in &partitions {
                if let Ok(reader) =
                    FixedColumnReader::open(&part.join("price.d"), ColumnType::F64)
                {
                    for i in 0..reader.row_count() {
                        total += reader.read_f64(black_box(i));
                    }
                }
            }
            black_box(total);
        });
    });

    // Pruned: only first partition.
    if let Some(first_part) = partitions.first() {
        let reader =
            FixedColumnReader::open(&first_part.join("price.d"), ColumnType::F64)
                .unwrap();
        let part_rows = reader.row_count();
        group.throughput(Throughput::Elements(part_rows));

        group.bench_function("pruned_1_of_30_partitions", |b| {
            b.iter(|| {
                let mut total = 0.0_f64;
                for i in 0..part_rows {
                    total += reader.read_f64(black_box(i));
                }
                black_box(total);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Bitmap index lookup vs full scan
// ---------------------------------------------------------------------------

fn bench_index_lookup(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let num_rows: u64 = MILLION;
    let num_keys: i32 = 1000;

    // Build bitmap index.
    {
        let mut idx =
            BitmapIndexWriter::open_default(dir.path(), "symbol_idx").unwrap();
        for row_id in 0..num_rows {
            let key = (row_id % num_keys as u64) as i32;
            idx.add(key, row_id).unwrap();
        }
        idx.flush().unwrap();
    }

    // Also write a column file so we can do a full-scan comparison.
    {
        let path = dir.path().join("symbol.d");
        let mut writer = FixedColumnWriter::open(&path, ColumnType::I32).unwrap();
        for i in 0..num_rows {
            writer.append_i32((i % num_keys as u64) as i32).unwrap();
        }
        writer.flush().unwrap();
    }

    let reader = BitmapIndexReader::open(dir.path(), "symbol_idx").unwrap();
    let col_reader = FixedColumnReader::open(
        &dir.path().join("symbol.d"),
        ColumnType::I32,
    )
    .unwrap();

    let mut group = c.benchmark_group("index_vs_full_scan");
    group.sample_size(20);

    // Bitmap index lookup for a single key.
    group.throughput(Throughput::Elements(num_rows / num_keys as u64));
    group.bench_function("bitmap_index_lookup_1_key", |b| {
        b.iter(|| {
            let row_ids = reader.get_row_ids(black_box(42));
            black_box(row_ids.len());
        });
    });

    // Full column scan for the same key.
    group.throughput(Throughput::Elements(num_rows));
    group.bench_function("full_scan_for_1_key", |b| {
        b.iter(|| {
            let target: i32 = 42;
            let mut count = 0u64;
            for i in 0..num_rows {
                if col_reader.read_i32(black_box(i)) == target {
                    count += 1;
                }
            }
            black_box(count);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. Compression ratio and speed (LZ4)
// ---------------------------------------------------------------------------

fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("lz4_compression");
    group.sample_size(20);

    // -- f64 price data (moderate compressibility) --------------------------
    {
        let f64_data: Vec<u8> = {
            let mut price = 50_000.0_f64;
            (0..MILLION)
                .flat_map(|i| {
                    let delta = ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64
                        * 0.5
                        - 2.5;
                    price += delta;
                    price.to_le_bytes()
                })
                .collect()
        };

        let data_len = f64_data.len() as u64;
        group.throughput(Throughput::Bytes(data_len));

        group.bench_function("compress_1M_f64", |b| {
            b.iter(|| {
                let compressed = lz4_flex::compress_prepend_size(black_box(&f64_data));
                black_box(compressed.len());
            });
        });

        let compressed = lz4_flex::compress_prepend_size(&f64_data);
        let compressed_len = compressed.len();
        group.throughput(Throughput::Bytes(compressed_len as u64));

        group.bench_function("decompress_1M_f64", |b| {
            b.iter(|| {
                let decompressed =
                    lz4_flex::decompress_size_prepended(black_box(&compressed))
                        .unwrap();
                black_box(decompressed.len());
            });
        });

        // Report ratio via a trivial benchmark that just computes it.
        let ratio = compressed_len as f64 / data_len as f64;
        eprintln!(
            "[lz4] f64 price data: {data_len} -> {compressed_len} bytes, ratio = {ratio:.3}"
        );
    }

    // -- i64 timestamp data (very compressible due to constant stride) ------
    {
        let ts_data: Vec<u8> = {
            let base: i64 = 1_704_067_200_000_000_000;
            (0..MILLION as i64)
                .flat_map(|i| (base + i * 1_000_000_000).to_le_bytes())
                .collect()
        };

        let data_len = ts_data.len() as u64;
        group.throughput(Throughput::Bytes(data_len));

        group.bench_function("compress_1M_timestamps", |b| {
            b.iter(|| {
                let compressed = lz4_flex::compress_prepend_size(black_box(&ts_data));
                black_box(compressed.len());
            });
        });

        let compressed = lz4_flex::compress_prepend_size(&ts_data);
        let compressed_len = compressed.len();
        let ratio = compressed_len as f64 / data_len as f64;
        eprintln!(
            "[lz4] timestamp data: {data_len} -> {compressed_len} bytes, ratio = {ratio:.3}"
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. Symbol map lookup at scale
// ---------------------------------------------------------------------------

fn bench_symbol_map_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("symbol_map_scale");
    group.sample_size(10);

    for num_symbols in [10_000u64, 100_000] {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "scale_sym").unwrap();
        let symbols: Vec<String> =
            (0..num_symbols).map(|i| format!("SYM_{:06}", i)).collect();
        for s in &symbols {
            sm.add(s).unwrap();
        }

        let lookups = 1_000_000u64;
        group.throughput(Throughput::Elements(lookups));

        group.bench_with_input(
            BenchmarkId::new("lookup", num_symbols),
            &num_symbols,
            |b, _| {
                b.iter(|| {
                    for i in 0..lookups as usize {
                        let sym = &symbols[i % symbols.len()];
                        black_box(sm.get_id(sym));
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 8. Concurrent column read + write
// ---------------------------------------------------------------------------

fn bench_concurrent_rw(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_rw");
    group.throughput(Throughput::Elements(HUNDRED_K));
    group.sample_size(10);

    group.bench_function("100K_write_with_concurrent_reads", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                setup_trades_table(dir.path(), HUNDRED_K, 1);
                dir
            },
            |dir| {
                let table_dir = dir.path().join("trades").join("default");

                // Reader thread: repeatedly scan the price column.
                let reader_dir = table_dir.clone();
                let reader_handle = std::thread::spawn(move || {
                    let mut read_sum = 0.0_f64;
                    for _ in 0..10 {
                        if let Ok(reader) = FixedColumnReader::open(
                            &reader_dir.join("price.d"),
                            ColumnType::F64,
                        ) {
                            for i in 0..reader.row_count() {
                                read_sum += reader.read_f64(i);
                            }
                        }
                    }
                    read_sum
                });

                // Writer: insert more rows into the table.
                let mut writer =
                    TableWriter::open(dir.path(), "trades").unwrap();
                let base_ts: i64 =
                    1_704_067_200_000_000_000 + HUNDRED_K as i64 * 1_000_000;
                let mut price = 50_000.0_f64;

                for i in 0..HUNDRED_K {
                    let ts = Timestamp(base_ts + i as i64 * 1_000_000);
                    price += 0.01;
                    writer
                        .write_row(
                            ts,
                            &[
                                ColumnValue::I32((i % 100) as i32),
                                ColumnValue::F64(price),
                                ColumnValue::F64(1.0),
                                ColumnValue::I32(0),
                            ],
                        )
                        .unwrap();
                }
                writer.flush().unwrap();

                let reads = reader_handle.join().unwrap();
                black_box(reads);
            },
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 9. Delta encoding throughput (timestamps)
// ---------------------------------------------------------------------------

fn bench_delta_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("delta_encoding");
    group.throughput(Throughput::Elements(MILLION));

    let timestamps: Vec<i64> = {
        let base: i64 = 1_704_067_200_000_000_000;
        (0..MILLION as i64).map(|i| base + i * 1_000_000_000).collect()
    };

    group.bench_function("encode_1M_timestamps", |b| {
        b.iter(|| {
            let encoded = compression::delta_encode_i64(black_box(&timestamps));
            black_box(encoded);
        });
    });

    let encoded = compression::delta_encode_i64(&timestamps);

    group.bench_function("decode_1M_timestamps", |b| {
        b.iter(|| {
            let decoded =
                compression::delta_decode_i64_nonempty(black_box(&encoded));
            black_box(decoded);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 10. SIMD aggregation vs scalar
// ---------------------------------------------------------------------------

fn bench_simd_vs_scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_vs_scalar");
    group.throughput(Throughput::Elements(MILLION));

    let data: Vec<f64> = (0..MILLION).map(|i| i as f64 * 0.001).collect();

    group.bench_function("simd_sum_f64", |b| {
        b.iter(|| {
            black_box(simd::sum_f64(black_box(&data)));
        });
    });

    group.bench_function("scalar_sum_f64", |b| {
        b.iter(|| {
            let sum: f64 = data.iter().sum();
            black_box(sum);
        });
    });

    group.bench_function("simd_min_f64", |b| {
        b.iter(|| {
            black_box(simd::min_f64(black_box(&data)));
        });
    });

    group.bench_function("scalar_min_f64", |b| {
        b.iter(|| {
            let min = data
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
            black_box(min);
        });
    });

    group.bench_function("simd_max_f64", |b| {
        b.iter(|| {
            black_box(simd::max_f64(black_box(&data)));
        });
    });

    group.bench_function("scalar_max_f64", |b| {
        b.iter(|| {
            let max = data
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            black_box(max);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 11. Batch write throughput (columnar bulk API)
// ---------------------------------------------------------------------------

fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write_throughput");
    group.sample_size(10);

    for num_rows in [HUNDRED_K, 500_000, MILLION] {
        group.throughput(Throughput::Elements(num_rows));
        group.bench_with_input(
            BenchmarkId::new("rows", num_rows),
            &num_rows,
            |b, &rows| {
                b.iter_with_setup(
                    || {
                        let dir = TempDir::new().unwrap();
                        let _meta = create_empty_trades(dir.path());

                        // Pre-build columnar data
                        let n = rows as usize;
                        let base_ts: i64 = 1_704_067_200_000_000_000;

                        let timestamps: Vec<i64> =
                            (0..n).map(|i| base_ts + i as i64 * 1_000_000).collect();

                        let symbols: Vec<i32> =
                            (0..n).map(|i| (i % 1000) as i32).collect();

                        let mut price = 50_000.0_f64;
                        let prices: Vec<f64> = (0..n)
                            .map(|i| {
                                let delta =
                                    ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64 * 0.5 - 2.5;
                                price += delta;
                                price
                            })
                            .collect();

                        let volumes: Vec<f64> =
                            (0..n).map(|i| 0.01 + (i % 10_000) as f64 * 0.01).collect();

                        let sides: Vec<i32> =
                            (0..n).map(|i| (i % 2) as i32).collect();

                        (dir, timestamps, symbols, prices, volumes, sides)
                    },
                    |(dir, timestamps, symbols, prices, volumes, sides)| {
                        let mut writer =
                            TableWriter::open(dir.path(), "trades").unwrap();

                        let sym_bytes = unsafe {
                            std::slice::from_raw_parts(
                                symbols.as_ptr() as *const u8,
                                symbols.len() * 4,
                            )
                        };
                        let price_bytes = unsafe {
                            std::slice::from_raw_parts(
                                prices.as_ptr() as *const u8,
                                prices.len() * 8,
                            )
                        };
                        let vol_bytes = unsafe {
                            std::slice::from_raw_parts(
                                volumes.as_ptr() as *const u8,
                                volumes.len() * 8,
                            )
                        };
                        let side_bytes = unsafe {
                            std::slice::from_raw_parts(
                                sides.as_ptr() as *const u8,
                                sides.len() * 4,
                            )
                        };

                        let written = writer
                            .write_batch(
                                black_box(&timestamps),
                                &[
                                    ("symbol", sym_bytes),
                                    ("price", price_bytes),
                                    ("volume", vol_bytes),
                                    ("side", side_bytes),
                                ],
                            )
                            .unwrap();
                        writer.flush().unwrap();
                        black_box(written);
                    },
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 11b. Batch write throughput using write_batch_raw (index-based, zero name lookup)
// ---------------------------------------------------------------------------

fn bench_batch_write_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write_raw_throughput");
    group.sample_size(10);

    for num_rows in [HUNDRED_K, 500_000, MILLION] {
        group.throughput(Throughput::Elements(num_rows));
        group.bench_with_input(
            BenchmarkId::new("rows", num_rows),
            &num_rows,
            |b, &rows| {
                b.iter_with_setup(
                    || {
                        let dir = TempDir::new().unwrap();
                        let _meta = create_empty_trades(dir.path());

                        let n = rows as usize;
                        let base_ts: i64 = 1_704_067_200_000_000_000;

                        let timestamps: Vec<i64> =
                            (0..n).map(|i| base_ts + i as i64 * 1_000_000).collect();

                        let symbols: Vec<i32> =
                            (0..n).map(|i| (i % 1000) as i32).collect();

                        let mut price = 50_000.0_f64;
                        let prices: Vec<f64> = (0..n)
                            .map(|i| {
                                let delta =
                                    ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64 * 0.5 - 2.5;
                                price += delta;
                                price
                            })
                            .collect();

                        let volumes: Vec<f64> =
                            (0..n).map(|i| 0.01 + (i % 10_000) as f64 * 0.01).collect();

                        let sides: Vec<i32> =
                            (0..n).map(|i| (i % 2) as i32).collect();

                        (dir, timestamps, symbols, prices, volumes, sides)
                    },
                    |(dir, timestamps, symbols, prices, volumes, sides)| {
                        let mut writer =
                            TableWriter::open(dir.path(), "trades").unwrap();

                        let sym_bytes = unsafe {
                            std::slice::from_raw_parts(
                                symbols.as_ptr() as *const u8,
                                symbols.len() * 4,
                            )
                        };
                        let price_bytes = unsafe {
                            std::slice::from_raw_parts(
                                prices.as_ptr() as *const u8,
                                prices.len() * 8,
                            )
                        };
                        let vol_bytes = unsafe {
                            std::slice::from_raw_parts(
                                volumes.as_ptr() as *const u8,
                                volumes.len() * 8,
                            )
                        };
                        let side_bytes = unsafe {
                            std::slice::from_raw_parts(
                                sides.as_ptr() as *const u8,
                                sides.len() * 4,
                            )
                        };

                        // Use column indices: symbol=1, price=2, volume=3, side=4
                        let written = writer
                            .write_batch_raw(
                                black_box(&timestamps),
                                &[
                                    (1, sym_bytes),
                                    (2, price_bytes),
                                    (3, vol_bytes),
                                    (4, side_bytes),
                                ],
                            )
                            .unwrap();
                        writer.flush().unwrap();
                        black_box(written);
                    },
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 12. Write with pre-opened writer (measures steady-state, no partition open cost)
// ---------------------------------------------------------------------------

fn bench_write_preallocated(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_preallocated");
    group.sample_size(10);
    group.throughput(Throughput::Elements(MILLION));

    group.bench_function("1M_rows_single_partition", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                // Use PartitionBy::None so all rows go to one partition
                let _meta = TableBuilder::new("trades")
                    .column("timestamp", ColumnType::Timestamp)
                    .column("symbol", ColumnType::I32)
                    .column("price", ColumnType::F64)
                    .column("volume", ColumnType::F64)
                    .column("side", ColumnType::I32)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::None)
                    .build(dir.path())
                    .expect("create table");

                let mut writer = TableWriter::open(dir.path(), "trades").unwrap();
                // Pre-open the partition by writing one row
                let ts = Timestamp(1_704_067_200_000_000_000);
                writer
                    .write_row(
                        ts,
                        &[
                            ColumnValue::I32(0),
                            ColumnValue::F64(50000.0),
                            ColumnValue::F64(1.0),
                            ColumnValue::I32(0),
                        ],
                    )
                    .unwrap();
                (dir, writer)
            },
            |(dir, mut writer)| {
                let base_ts: i64 = 1_704_067_200_000_000_000 + 1_000_000;
                let mut price = 50_000.0_f64;

                for i in 0..MILLION {
                    let ts = Timestamp(base_ts + i as i64 * 1_000_000);
                    let delta =
                        ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64 * 0.5 - 2.5;
                    price += delta;

                    writer
                        .write_row(
                            ts,
                            &[
                                ColumnValue::I32((i % 1000) as i32),
                                ColumnValue::F64(black_box(price)),
                                ColumnValue::F64(black_box(
                                    0.01 + (i % 10_000) as f64 * 0.01,
                                )),
                                ColumnValue::I32((i % 2) as i32),
                            ],
                        )
                        .unwrap();
                }
                writer.flush().unwrap();
                let _ = dir; // keep dir alive
            },
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 13. WAL write throughput with deferred merge
// ---------------------------------------------------------------------------

fn bench_wal_write_deferred(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_write_deferred_merge");
    group.throughput(Throughput::Elements(HUNDRED_K));
    group.sample_size(10);

    group.bench_function("100K_rows_no_merge", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let _meta = TableBuilder::new("wal_bench")
                    .column("timestamp", ColumnType::Timestamp)
                    .column("price", ColumnType::F64)
                    .column("volume", ColumnType::I64)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::Day)
                    .build(dir.path())
                    .unwrap();

                let table_dir = dir.path().join("wal_bench");
                let _txn =
                    exchange_core::txn::TxnFile::open(&table_dir).unwrap();

                dir
            },
            |dir| {
                let config = WalTableWriterConfig {
                    buffer_capacity: 50_000,
                    merge_on_commit: false, // <-- deferred merge
                    ..Default::default()
                };
                let mut writer =
                    WalTableWriter::open(dir.path(), "wal_bench", config).unwrap();

                let base_ts: i64 = 1_704_067_200_000_000_000;
                for i in 0..HUNDRED_K as i64 {
                    let ts_val = base_ts + i * 1_000_000;
                    let ts = Timestamp(ts_val);
                    writer
                        .write_row(
                            ts,
                            vec![
                                OwnedColumnValue::Timestamp(ts_val),
                                OwnedColumnValue::F64(50_000.0 + i as f64 * 0.01),
                                OwnedColumnValue::I64(i),
                            ],
                        )
                        .unwrap();
                }
                writer.commit().unwrap();
            },
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_table_writer,
    bench_table_reader,
    bench_wal_write,
    bench_batch_write,
    bench_batch_write_raw,
    bench_write_preallocated,
    bench_wal_write_deferred,
    bench_partition_pruning,
    bench_index_lookup,
    bench_compression,
    bench_symbol_map_scale,
    bench_concurrent_rw,
    bench_delta_encoding,
    bench_simd_vs_scalar,
);
criterion_main!(benches);
