use criterion::{black_box, criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use exchange_common::types::ColumnType;
use exchange_core::column::{FixedColumnReader, FixedColumnWriter, VarColumnWriter};
use exchange_core::index::symbol_map::SymbolMap;
use exchange_core::mmap::MmapFile;
use exchange_common::ringbuf::SpscRingBuffer;
use tempfile::tempdir;

const MILLION: u64 = 1_000_000;
const HUNDRED_K: u64 = 100_000;

fn column_write_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_write_f64");
    group.throughput(Throughput::Elements(MILLION));

    group.bench_function("1M_f64", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("price.d");
                let writer = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
                (dir, writer)
            },
            |(_dir, mut writer)| {
                for i in 0..MILLION {
                    writer.append_f64(black_box(i as f64 * 0.01)).unwrap();
                }
            },
        );
    });

    group.finish();
}

fn column_read_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_read_f64");
    group.throughput(Throughput::Elements(MILLION));

    // Prepare data on disk once.
    let dir = tempdir().unwrap();
    let path = dir.path().join("price.d");
    {
        let mut writer = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
        for i in 0..MILLION {
            writer.append_f64(i as f64 * 0.01).unwrap();
        }
        writer.flush().unwrap();
    }

    let reader = FixedColumnReader::open(&path, ColumnType::F64).unwrap();

    group.bench_function("1M_f64", |b| {
        b.iter(|| {
            let mut sum = 0.0_f64;
            for i in 0..MILLION {
                sum += reader.read_f64(black_box(i));
            }
            black_box(sum);
        });
    });

    group.bench_function("1M_f64_slice", |b| {
        b.iter(|| {
            let slice = reader.as_f64_slice();
            let mut sum = 0.0_f64;
            for &v in slice {
                sum += v;
            }
            black_box(sum);
        });
    });

    group.finish();
}

fn column_write_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_write_i64");
    group.throughput(Throughput::Elements(MILLION));

    group.bench_function("1M_i64", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("ts.d");
                let writer = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
                (dir, writer)
            },
            |(_dir, mut writer)| {
                for i in 0..MILLION as i64 {
                    writer.append_i64(black_box(i * 1_000_000_000)).unwrap();
                }
            },
        );
    });

    group.finish();
}

fn var_column_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_column_write");
    group.throughput(Throughput::Elements(HUNDRED_K));

    // Pre-generate strings of varying lengths (10-50 bytes).
    let strings: Vec<String> = (0..HUNDRED_K as usize)
        .map(|i| {
            let len = 10 + (i % 41); // 10..50 bytes
            format!("{:>width$}", i, width = len)
        })
        .collect();

    group.bench_function("100K_strings", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let data_path = dir.path().join("col.d");
                let index_path = dir.path().join("col.i");
                let writer = VarColumnWriter::open(&data_path, &index_path).unwrap();
                (dir, writer)
            },
            |(_dir, mut writer)| {
                for s in &strings {
                    writer.append_str(black_box(s.as_str())).unwrap();
                }
            },
        );
    });

    group.finish();
}

fn symbol_map_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("symbol_map_lookup");
    group.throughput(Throughput::Elements(MILLION));

    let dir = tempdir().unwrap();
    let mut sm = SymbolMap::open(dir.path(), "bench_sym").unwrap();
    let symbols: Vec<String> = (0..1000).map(|i| format!("SYM_{}", i)).collect();
    for s in &symbols {
        sm.add(s).unwrap();
    }

    group.bench_function("1M_lookups_1000_symbols", |b| {
        b.iter(|| {
            for i in 0..MILLION as usize {
                let sym = &symbols[i % symbols.len()];
                black_box(sm.get_id(sym));
            }
        });
    });

    group.finish();
}

fn symbol_map_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("symbol_map_insert");
    group.throughput(Throughput::Elements(HUNDRED_K));

    // Pre-generate symbol names.
    let symbols: Vec<String> = (0..HUNDRED_K as usize)
        .map(|i| format!("INSERT_SYM_{}", i))
        .collect();

    group.bench_function("100K_inserts", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let sm = SymbolMap::open(dir.path(), "bench_ins").unwrap();
                (dir, sm)
            },
            |(_dir, mut sm)| {
                for s in &symbols {
                    sm.add(black_box(s.as_str())).unwrap();
                }
            },
        );
    });

    group.finish();
}

fn mmap_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("mmap_append");
    group.throughput(Throughput::Elements(MILLION));

    group.bench_function("1M_8byte_chunks", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("mmap_bench.d");
                let mf = MmapFile::open(&path, 8 * MILLION).unwrap();
                (dir, mf)
            },
            |(_dir, mut mf)| {
                let chunk = 42u64.to_le_bytes();
                for _ in 0..MILLION {
                    mf.append(black_box(&chunk)).unwrap();
                }
            },
        );
    });

    group.finish();
}

fn ringbuf_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("ringbuf_throughput");
    group.throughput(Throughput::Elements(MILLION));

    group.bench_function("1M_push_pop", |b| {
        b.iter_with_setup(
            || SpscRingBuffer::<u64>::new(1024),
            |rb| {
                for i in 0..MILLION {
                    // Push, and if full, pop first then push.
                    while rb.try_push(black_box(i)).is_err() {
                        black_box(rb.try_pop());
                    }
                }
                // Drain remaining.
                while rb.try_pop().is_some() {}
            },
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    column_write_f64,
    column_read_f64,
    column_write_i64,
    var_column_write,
    symbol_map_lookup,
    symbol_map_insert,
    mmap_append,
    ringbuf_throughput,
);
criterion_main!(benches);
