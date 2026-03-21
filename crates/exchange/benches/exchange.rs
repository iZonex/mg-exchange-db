use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use exchange_exchange::ohlcv::{OhlcvAggregator, TimeFrame};
use exchange_exchange::orderbook::{BookSide, DeltaAction, OrderBookDelta, OrderBookStore};
use exchange_exchange::tick::{delta_decode_prices, delta_encode_prices};

const MILLION: u64 = 1_000_000;
const HUNDRED_K: u64 = 100_000;
#[allow(dead_code)]
const SECOND_NS: i64 = 1_000_000_000;

fn ohlcv_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("ohlcv_aggregation");
    group.throughput(Throughput::Elements(MILLION));

    // Pre-generate tick data: 1M ticks spread over many 1-second bars.
    let ticks: Vec<(f64, f64, i64)> = (0..MILLION as i64)
        .map(|i| {
            let price = 50000.0 + (i % 1000) as f64 * 0.01;
            let volume = 0.1 + (i % 50) as f64 * 0.01;
            // Each tick is 1ms apart, so bars flip every 1000 ticks.
            let ts = i * 1_000_000; // 1ms in nanos
            (price, volume, ts)
        })
        .collect();

    group.bench_function("1M_ticks_S1", |b| {
        b.iter(|| {
            let mut agg = OhlcvAggregator::new(TimeFrame::S1);
            let mut bar_count = 0u64;
            for &(price, volume, ts) in &ticks {
                if agg
                    .update(black_box(price), black_box(volume), black_box(ts))
                    .is_some()
                {
                    bar_count += 1;
                }
            }
            if agg.flush().is_some() {
                bar_count += 1;
            }
            black_box(bar_count);
        });
    });

    group.finish();
}

fn orderbook_delta_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("orderbook_delta_apply");
    group.throughput(Throughput::Elements(HUNDRED_K));

    // Pre-generate 100K deltas: mix of adds, modifies, and deletes.
    let deltas: Vec<OrderBookDelta> = (0..HUNDRED_K as usize)
        .map(|i| {
            let price = 50000.0 + (i % 500) as f64 * 0.01;
            let side = if i % 2 == 0 {
                BookSide::Bid
            } else {
                BookSide::Ask
            };
            let action = match i % 10 {
                0..=5 => DeltaAction::Add,
                6..=8 => DeltaAction::Modify,
                _ => DeltaAction::Delete,
            };
            OrderBookDelta {
                action,
                side,
                price,
                quantity: (i % 100) as f64 * 0.1,
                order_count: (i % 20) as u32,
            }
        })
        .collect();

    group.bench_function("100K_deltas", |b| {
        b.iter_with_setup(
            || OrderBookStore::new("BENCH/USD"),
            |mut store| {
                for d in &deltas {
                    store.apply_delta(black_box(d));
                }
                black_box(&store);
            },
        );
    });

    group.finish();
}

fn tick_delta_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("tick_delta_encode");
    group.throughput(Throughput::Elements(MILLION));

    // Pre-generate 1M prices simulating a random walk.
    let prices: Vec<f64> = {
        let mut v = Vec::with_capacity(MILLION as usize);
        let mut price = 50000.0_f64;
        for i in 0..MILLION as usize {
            // Deterministic small fluctuations.
            let delta = ((i * 7 + 3) % 11) as f64 * 0.01 - 0.05;
            price += delta;
            v.push(price);
        }
        v
    };

    group.bench_function("1M_prices", |b| {
        b.iter(|| {
            let result = delta_encode_prices(black_box(&prices), 2);
            black_box(result);
        });
    });

    group.finish();
}

fn tick_delta_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("tick_delta_decode");
    group.throughput(Throughput::Elements(MILLION));

    // Pre-encode data for decoding benchmark.
    let prices: Vec<f64> = {
        let mut v = Vec::with_capacity(MILLION as usize);
        let mut price = 50000.0_f64;
        for i in 0..MILLION as usize {
            let delta = ((i * 7 + 3) % 11) as f64 * 0.01 - 0.05;
            price += delta;
            v.push(price);
        }
        v
    };
    let (base, deltas) = delta_encode_prices(&prices, 2);

    group.bench_function("1M_prices", |b| {
        b.iter(|| {
            let result = delta_decode_prices(black_box(base), black_box(&deltas), 2);
            black_box(result);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    ohlcv_aggregation,
    orderbook_delta_apply,
    tick_delta_encode,
    tick_delta_decode,
);
criterion_main!(benches);
