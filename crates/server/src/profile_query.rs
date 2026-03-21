//! Exchange performance profiler — queries + order book operations.

use std::time::Instant;

fn main() {
    let data_dir = std::path::PathBuf::from("/tmp/exchangedb-demo");
    let n = 500u32;

    println!("=== ExchangeDB Performance Profiler ===\n");

    // Initialize table registry
    exchange_query::table_registry::init_global(data_dir.clone());

    // ── SQL Query Performance ──
    let queries = [
        ("SELECT * LIMIT 1", "SELECT * FROM ohlcv LIMIT 1"),
        ("SELECT * LIMIT 25", "SELECT * FROM ohlcv LIMIT 25"),
        ("SELECT * LIMIT 100", "SELECT * FROM ohlcv LIMIT 100"),
        ("SELECT * (all 504)", "SELECT * FROM ohlcv"),
        ("COUNT(*)", "SELECT count(*) FROM ohlcv"),
        ("SAMPLE BY 4h", "SELECT first(open) AS o, max(high) AS h, min(low) AS l, last(close) AS c, sum(volume) AS v FROM ohlcv SAMPLE BY 4h"),
        ("LATEST ON", "SELECT * FROM ohlcv LATEST ON timestamp PARTITION BY symbol"),
    ];

    // Warmup
    for (_, sql) in &queries {
        let plan = exchange_query::plan_query(sql).unwrap();
        let _ = exchange_query::execute(&data_dir, &plan);
    }

    println!("--- SQL Queries (via execute()) ---");
    for (name, sql) in &queries {
        let plan = exchange_query::plan_query(sql).unwrap();
        let times = bench(n, || {
            let _ = std::hint::black_box(exchange_query::execute(&data_dir, &plan));
        });
        println!("{:25} p50={:>10?}  min={:>10?}", name, times[n as usize/2], times[0]);
    }

    // ── Order Book Performance ──
    println!("\n--- Order Book Operations ---");

    use exchange_exchange::orderbook::*;

    let ob_n = 100_000u32;

    // Build realistic book: 50 bid + 50 ask levels
    let mut store = OrderBookStore::new("BTC/USD");
    for i in 0..50u32 {
        store.apply_delta(&OrderBookDelta {
            action: DeltaAction::Add, side: BookSide::Bid,
            price: 65000.0 - i as f64 * 10.0,
            quantity: 1.0 + i as f64 * 0.5, order_count: 3 + i,
        });
        store.apply_delta(&OrderBookDelta {
            action: DeltaAction::Add, side: BookSide::Ask,
            price: 65010.0 + i as f64 * 10.0,
            quantity: 0.5 + i as f64 * 0.3, order_count: 2 + i,
        });
    }

    // apply_delta
    let times = bench(ob_n, || {
        store.apply_delta(&OrderBookDelta {
            action: DeltaAction::Modify, side: BookSide::Bid,
            price: 64950.0, quantity: 2.0, order_count: 5,
        });
    });
    println!("{:25} p50={:>10?}  min={:>10?}  p99={:>10?}", "apply_delta", times[ob_n as usize/2], times[0], times[(ob_n as f64*0.99) as usize]);

    // best_bid
    let times = bench(ob_n, || { let _ = std::hint::black_box(store.best_bid()); });
    println!("{:25} p50={:>10?}  min={:>10?}", "best_bid", times[ob_n as usize/2], times[0]);

    // best_ask
    let times = bench(ob_n, || { let _ = std::hint::black_box(store.best_ask()); });
    println!("{:25} p50={:>10?}  min={:>10?}", "best_ask", times[ob_n as usize/2], times[0]);

    // spread
    let times = bench(ob_n, || { let _ = std::hint::black_box(store.spread()); });
    println!("{:25} p50={:>10?}  min={:>10?}", "spread", times[ob_n as usize/2], times[0]);

    // mid_price
    let times = bench(ob_n, || { let _ = std::hint::black_box(store.mid_price()); });
    println!("{:25} p50={:>10?}  min={:>10?}", "mid_price", times[ob_n as usize/2], times[0]);

    // snapshot (50 bids + 50 asks)
    let times = bench(10_000, || { let _ = std::hint::black_box(store.current_snapshot(0)); });
    println!("{:25} p50={:>10?}  min={:>10?}", "snapshot(100 levels)", times[5000], times[0]);

    // apply batch of 10 deltas
    let batch: Vec<OrderBookDelta> = (0..10).map(|i| OrderBookDelta {
        action: DeltaAction::Modify,
        side: if i % 2 == 0 { BookSide::Bid } else { BookSide::Ask },
        price: 65000.0 - (i % 50) as f64 * 10.0,
        quantity: 2.0, order_count: 5,
    }).collect();
    let times = bench(ob_n, || { store.apply_deltas(&batch, 1); });
    println!("{:25} p50={:>10?}  min={:>10?}", "apply_batch(10)", times[ob_n as usize/2], times[0]);

    // apply batch of 100 deltas
    let batch100: Vec<OrderBookDelta> = (0..100).map(|i| OrderBookDelta {
        action: DeltaAction::Modify,
        side: if i % 2 == 0 { BookSide::Bid } else { BookSide::Ask },
        price: 65000.0 - (i % 50) as f64 * 10.0,
        quantity: 2.0, order_count: 5,
    }).collect();
    let times = bench(ob_n, || { store.apply_deltas(&batch100, 1); });
    println!("{:25} p50={:>10?}  min={:>10?}", "apply_batch(100)", times[ob_n as usize/2], times[0]);

    println!("\n--- Throughput ---");
    let start = Instant::now();
    let ops = 1_000_000u64;
    for i in 0..ops {
        store.apply_delta(&OrderBookDelta {
            action: DeltaAction::Modify,
            side: if i % 2 == 0 { BookSide::Bid } else { BookSide::Ask },
            price: 65000.0 - (i % 50) as f64 * 10.0,
            quantity: (i % 100) as f64 * 0.1, order_count: (i % 20 + 1) as u32,
        });
    }
    let elapsed = start.elapsed();
    println!("1M delta applies: {:?} ({:.1}M ops/sec)", elapsed, ops as f64 / elapsed.as_secs_f64() / 1_000_000.0);
}

fn bench(n: u32, mut f: impl FnMut()) -> Vec<std::time::Duration> {
    let mut times = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let t = Instant::now();
        f();
        times.push(t.elapsed());
    }
    times.sort();
    times
}
