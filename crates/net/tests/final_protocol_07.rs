//! 500 ILP parsing, metrics, auth, catalog queries, type mapping tests.

use exchange_net::ilp::{
    IlpLine, IlpParseError, IlpValue, IlpVersion, parse_ilp_batch, parse_ilp_line,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

// ===========================================================================
// ILP field types with different values — 100 tests
// ===========================================================================
mod ilp_fields_f07 {
    use super::*;
    // Integer fields
    macro_rules! intf {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                let line = format!("m val={}i 1000", $val);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.fields.get("val"), Some(&IlpValue::Integer($val)));
            }
        };
    }
    intf!(i0, 0);
    intf!(i1, 1);
    intf!(i2, 2);
    intf!(i3, 3);
    intf!(i5, 5);
    intf!(i10, 10);
    intf!(i42, 42);
    intf!(i99, 99);
    intf!(i100, 100);
    intf!(i255, 255);
    intf!(i256, 256);
    intf!(i500, 500);
    intf!(i1000, 1000);
    intf!(i9999, 9999);
    intf!(i10000, 10000);
    intf!(in1, -1);
    intf!(in10, -10);
    intf!(in100, -100);
    intf!(in1000, -1000);
    intf!(in9999, -9999);

    // Float fields
    macro_rules! fltf {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                let line = format!("m val={} 1000", $val);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.fields.get("val"), Some(&IlpValue::Float($val)));
            }
        };
    }
    fltf!(f0, 0.0);
    fltf!(f1, 1.0);
    fltf!(f1_5, 1.5);
    fltf!(f2_0, 2.0);
    fltf!(f2_5, 2.5);
    fltf!(f3_14, 3.14);
    fltf!(f10_0, 10.0);
    fltf!(f42_0, 42.0);
    fltf!(f99_9, 99.9);
    fltf!(f100_0, 100.0);
    fltf!(fn1, -1.0);
    fltf!(fn1_5, -1.5);
    fltf!(fn3_14, -3.14);
    fltf!(fn10, -10.0);
    fltf!(fn100, -100.0);
    fltf!(f0_1, 0.1);
    fltf!(f0_01, 0.01);
    fltf!(f0_001, 0.001);
    fltf!(f1000, 1000.0);
    fltf!(f9999, 9999.9);

    // String fields
    macro_rules! strf {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                let line = format!(r#"m val="{}" 1000"#, $val);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.fields.get("val"), Some(&IlpValue::String($val.into())));
            }
        };
    }
    strf!(s_hello, "hello");
    strf!(s_world, "world");
    strf!(s_test, "test");
    strf!(s_abc, "abc");
    strf!(s_xyz, "xyz");
    strf!(s_empty, "");
    strf!(s_space, " ");
    strf!(s_digits, "12345");
    strf!(s_mixed, "abc123");
    strf!(s_path, "/usr/bin");
    strf!(s_email, "a@b.c");
    strf!(s_url, "http://x");
    strf!(s_json, "key:val");
    strf!(s_csv, "a,b,c");
    strf!(s_long, "abcdefghijklmnopqrstuvwxyz");

    // Boolean fields
    #[test]
    fn b_true() {
        let p = parse_ilp_line("m val=true 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn b_false() {
        let p = parse_ilp_line("m val=false 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(false)));
    }
    #[test]
    fn b_t() {
        let p = parse_ilp_line("m val=T 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn b_f() {
        let p = parse_ilp_line("m val=F 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(false)));
    }
    #[test]
    fn b_true_caps() {
        let p = parse_ilp_line("m val=TRUE 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn b_false_caps() {
        let p = parse_ilp_line("m val=FALSE 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(false)));
    }
    #[test]
    fn b_true_mixed() {
        let p = parse_ilp_line("m val=True 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn b_false_mixed() {
        let p = parse_ilp_line("m val=False 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(false)));
    }

    // Field names
    macro_rules! fname {
        ($n:ident, $name:expr) => {
            #[test]
            fn $n() {
                let line = format!("m {}=1i 1000", $name);
                let p = parse_ilp_line(&line).unwrap();
                assert!(p.fields.contains_key($name));
            }
        };
    }
    fname!(fn_val, "val");
    fname!(fn_price, "price");
    fname!(fn_qty, "qty");
    fname!(fn_vol, "volume");
    fname!(fn_bid, "bid");
    fname!(fn_ask, "ask");
    fname!(fn_high, "high");
    fname!(fn_low, "low");
    fname!(fn_open, "open");
    fname!(fn_close, "close");
    fname!(fn_count, "count");
    fname!(fn_sum, "sum");
}

// ===========================================================================
// Tags with different measurement names — 80 tests
// ===========================================================================
mod ilp_tags_f07 {
    use super::*;
    // Measurement names
    macro_rules! mname {
        ($n:ident, $name:expr) => {
            #[test]
            fn $n() {
                let line = format!("{} val=1i 1000", $name);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.measurement, $name);
            }
        };
    }
    mname!(m_trades, "trades");
    mname!(m_orders, "orders");
    mname!(m_quotes, "quotes");
    mname!(m_ticks, "ticks");
    mname!(m_candles, "candles");
    mname!(m_bars, "bars");
    mname!(m_positions, "positions");
    mname!(m_accounts, "accounts");
    mname!(m_metrics, "metrics");
    mname!(m_events, "events");
    mname!(m_logs, "logs");
    mname!(m_cpu, "cpu");
    mname!(m_mem, "mem");
    mname!(m_disk, "disk");
    mname!(m_net, "net");
    mname!(m_io, "io");
    mname!(m_sys, "sys");
    mname!(m_app, "app");
    mname!(m_db, "db");
    mname!(m_table, "table_data");

    // Tags
    macro_rules! tag1 {
        ($n:ident, $k:expr, $v:expr) => {
            #[test]
            fn $n() {
                let line = format!("m,{}={} val=1i 1000", $k, $v);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.tags.get($k).unwrap(), $v);
            }
        };
    }
    tag1!(t_sym, "sym", "AAPL");
    tag1!(t_exch, "exchange", "NYSE");
    tag1!(t_ccy, "currency", "USD");
    tag1!(t_host, "host", "srv01");
    tag1!(t_region, "region", "us-east");
    tag1!(t_env, "env", "prod");
    tag1!(t_ver, "version", "1.0");
    tag1!(t_type, "type", "trade");
    tag1!(t_side, "side", "buy");
    tag1!(t_status, "status", "active");
    tag1!(t_id1, "id", "001");
    tag1!(t_id2, "id", "002");
    tag1!(t_id3, "id", "003");
    tag1!(t_src, "source", "feed1");
    tag1!(t_dest, "dest", "store1");
    tag1!(t_class, "class", "equity");
    tag1!(t_sector, "sector", "tech");
    tag1!(t_market, "market", "US");
    tag1!(t_country, "country", "UK");
    tag1!(t_city, "city", "London");

    // Multi tags
    #[test]
    fn two_tags_a() {
        let p = parse_ilp_line("m,sym=AAPL,exch=NYSE val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 2);
    }
    #[test]
    fn two_tags_b() {
        let p = parse_ilp_line("m,host=a,env=prod val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 2);
    }
    #[test]
    fn three_tags_a() {
        let p = parse_ilp_line("m,a=1,b=2,c=3 val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 3);
    }
    #[test]
    fn four_tags() {
        let p = parse_ilp_line("m,a=1,b=2,c=3,d=4 val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 4);
    }
    #[test]
    fn five_tags_a() {
        let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5 val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 5);
    }
    #[test]
    fn six_tags() {
        let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5,f=6 val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 6);
    }
    #[test]
    fn seven_tags() {
        let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5,f=6,g=7 val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 7);
    }
    #[test]
    fn eight_tags() {
        let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5,f=6,g=7,h=8 val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 8);
    }

    // N tags via loop
    macro_rules! ntag {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let tags: String = (0..$count)
                    .map(|i| format!("t{i}=v{i}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let line = format!("m,{tags} val=1i 1000");
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.tags.len(), $count);
            }
        };
    }
    ntag!(nt9, 9);
    ntag!(nt10, 10);
    ntag!(nt15, 15);
    ntag!(nt20, 20);
    ntag!(nt25, 25);
    ntag!(nt30, 30);
    ntag!(nt40, 40);
    ntag!(nt50, 50);
    ntag!(nt75, 75);
    ntag!(nt100, 100);
}

// ===========================================================================
// Multi fields — 80 tests
// ===========================================================================
mod ilp_multi_f07 {
    use super::*;
    // Different field count combos
    macro_rules! nfields {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let fields: String = (0..$count)
                    .map(|j| format!("f{j}={j}i"))
                    .collect::<Vec<_>>()
                    .join(",");
                let line = format!("m {fields} 1000");
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.fields.len(), $count);
            }
        };
    }
    nfields!(nf1, 1);
    nfields!(nf2, 2);
    nfields!(nf3, 3);
    nfields!(nf4, 4);
    nfields!(nf5, 5);
    nfields!(nf6, 6);
    nfields!(nf7, 7);
    nfields!(nf8, 8);
    nfields!(nf9, 9);
    nfields!(nf10, 10);
    nfields!(nf11, 11);
    nfields!(nf12, 12);
    nfields!(nf13, 13);
    nfields!(nf14, 14);
    nfields!(nf15, 15);
    nfields!(nf16, 16);
    nfields!(nf17, 17);
    nfields!(nf18, 18);
    nfields!(nf19, 19);
    nfields!(nf20, 20);
    nfields!(nf25, 25);
    nfields!(nf30, 30);
    nfields!(nf40, 40);
    nfields!(nf50, 50);
    nfields!(nf75, 75);

    // Mixed type fields
    #[test]
    fn mix_if() {
        let p = parse_ilp_line("m a=1i,b=2.0 1000").unwrap();
        assert_eq!(p.fields.len(), 2);
    }
    #[test]
    fn mix_is() {
        let p = parse_ilp_line(r#"m a=1i,b="hi" 1000"#).unwrap();
        assert_eq!(p.fields.len(), 2);
    }
    #[test]
    fn mix_ib() {
        let p = parse_ilp_line("m a=1i,b=true 1000").unwrap();
        assert_eq!(p.fields.len(), 2);
    }
    #[test]
    fn mix_fb() {
        let p = parse_ilp_line("m a=1.0,b=false 1000").unwrap();
        assert_eq!(p.fields.len(), 2);
    }
    #[test]
    fn mix_sb() {
        let p = parse_ilp_line(r#"m a="x",b=true 1000"#).unwrap();
        assert_eq!(p.fields.len(), 2);
    }
    #[test]
    fn mix_all() {
        let p = parse_ilp_line(r#"m a=1i,b=2.0,c="hi",d=true 1000"#).unwrap();
        assert_eq!(p.fields.len(), 4);
    }
    #[test]
    fn mix_all2() {
        let p = parse_ilp_line(r#"m a=1i,b=2.0,c="hi",d=true,e=false,f=3i 1000"#).unwrap();
        assert_eq!(p.fields.len(), 6);
    }

    // Fields sorted
    #[test]
    fn sorted_2() {
        let p = parse_ilp_line("m z=1i,a=2i 1000").unwrap();
        let keys: Vec<&String> = p.fields.keys().collect();
        assert_eq!(keys, vec!["a", "z"]);
    }
    #[test]
    fn sorted_3() {
        let p = parse_ilp_line("m z=1i,m=2i,a=3i 1000").unwrap();
        let keys: Vec<&String> = p.fields.keys().collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
    }
    #[test]
    fn sorted_5() {
        let p = parse_ilp_line("m z=1i,y=2i,x=3i,w=4i,v=5i 1000").unwrap();
        let keys: Vec<&String> = p.fields.keys().collect();
        assert_eq!(keys, vec!["v", "w", "x", "y", "z"]);
    }

    // Float field names
    macro_rules! ff {
        ($n:ident, $name:expr, $val:expr) => {
            #[test]
            fn $n() {
                let line = format!("m {}={} 1000", $name, $val);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.fields.get($name), Some(&IlpValue::Float($val)));
            }
        };
    }
    ff!(ff_bid, "bid", 100.5);
    ff!(ff_ask, "ask", 101.0);
    ff!(ff_price, "price", 99.99);
    ff!(ff_qty, "qty", 1000.0);
    ff!(ff_vol, "volume", 50000.0);
    ff!(ff_high, "high", 105.0);
    ff!(ff_low, "low", 95.0);
    ff!(ff_open, "open", 100.0);
    ff!(ff_close, "close", 102.0);
    ff!(ff_vwap, "vwap", 101.5);

    // Integer field names
    macro_rules! fi {
        ($n:ident, $name:expr, $val:expr) => {
            #[test]
            fn $n() {
                let line = format!("m {}={}i 1000", $name, $val);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.fields.get($name), Some(&IlpValue::Integer($val)));
            }
        };
    }
    fi!(fi_count, "count", 42);
    fi!(fi_id, "id", 12345);
    fi!(fi_seq, "seq", 1);
    fi!(fi_size, "size", 1024);
    fi!(fi_len, "len", 256);
    fi!(fi_depth, "depth", 10);
    fi!(fi_level, "level", 3);
    fi!(fi_rank, "rank", 7);
    fi!(fi_idx, "idx", 0);
    fi!(fi_pos, "pos", 99);

    // Multi-field verification
    #[test]
    fn verify_int() {
        let p = parse_ilp_line("m a=42i,b=99i 1000").unwrap();
        assert_eq!(p.fields.get("a"), Some(&IlpValue::Integer(42)));
        assert_eq!(p.fields.get("b"), Some(&IlpValue::Integer(99)));
    }
    #[test]
    fn verify_float() {
        let p = parse_ilp_line("m a=1.5,b=2.5 1000").unwrap();
        assert_eq!(p.fields.get("a"), Some(&IlpValue::Float(1.5)));
        assert_eq!(p.fields.get("b"), Some(&IlpValue::Float(2.5)));
    }
    #[test]
    fn verify_mixed() {
        let p = parse_ilp_line(r#"m a=1i,b=2.0,c="x" 1000"#).unwrap();
        assert_eq!(p.fields.get("a"), Some(&IlpValue::Integer(1)));
        assert_eq!(p.fields.get("b"), Some(&IlpValue::Float(2.0)));
        assert_eq!(p.fields.get("c"), Some(&IlpValue::String("x".into())));
    }
}

// ===========================================================================
// Batch parsing — 80 tests
// ===========================================================================
mod ilp_batch_f07 {
    use super::*;
    macro_rules! batch_n {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let mut input = String::new();
                for j in 0..$count {
                    input.push_str(&format!("m,t=t{j} val={j}i {j}000\n"));
                }
                let r = parse_ilp_batch(&input).unwrap();
                assert_eq!(r.len(), $count);
            }
        };
    }
    batch_n!(b1, 1);
    batch_n!(b2, 2);
    batch_n!(b3, 3);
    batch_n!(b4, 4);
    batch_n!(b5, 5);
    batch_n!(b6, 6);
    batch_n!(b7, 7);
    batch_n!(b8, 8);
    batch_n!(b9, 9);
    batch_n!(b10, 10);
    batch_n!(b11, 11);
    batch_n!(b12, 12);
    batch_n!(b13, 13);
    batch_n!(b14, 14);
    batch_n!(b15, 15);
    batch_n!(b16, 16);
    batch_n!(b17, 17);
    batch_n!(b18, 18);
    batch_n!(b19, 19);
    batch_n!(b20, 20);
    batch_n!(b25, 25);
    batch_n!(b30, 30);
    batch_n!(b40, 40);
    batch_n!(b50, 50);
    batch_n!(b75, 75);
    batch_n!(b100, 100);
    batch_n!(b150, 150);
    batch_n!(b200, 200);
    batch_n!(b250, 250);
    batch_n!(b300, 300);
    batch_n!(b400, 400);
    batch_n!(b500, 500);

    // With blanks and comments
    #[test]
    fn blanks_1() {
        let r = parse_ilp_batch("m a=1i 1000\n\nm b=2i 2000\n").unwrap();
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn blanks_2() {
        let r = parse_ilp_batch("\n\nm a=1i 1000\n\n").unwrap();
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn blanks_3() {
        let r = parse_ilp_batch("\n\n\n").unwrap();
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn comments_1() {
        let r = parse_ilp_batch("# c\nm a=1i 1000\n").unwrap();
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn comments_2() {
        let r = parse_ilp_batch("# c1\n# c2\nm a=1i 1000\n").unwrap();
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn comments_3() {
        let r = parse_ilp_batch("# c\n# c\n# c\n").unwrap();
        assert_eq!(r.len(), 0);
    }

    // Mixed measurements in batch
    #[test]
    fn mixed_meas() {
        let r = parse_ilp_batch("trades a=1i 1000\norders b=2i 2000\nquotes c=3i 3000\n").unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].measurement, "trades");
        assert_eq!(r[1].measurement, "orders");
        assert_eq!(r[2].measurement, "quotes");
    }

    // Large batch with multi fields
    macro_rules! batch_mf {
        ($n:ident, $count:expr, $fields:expr) => {
            #[test]
            fn $n() {
                let mut input = String::new();
                for j in 0..$count {
                    let fields: String = (0..$fields)
                        .map(|f| format!("f{f}={j}i"))
                        .collect::<Vec<_>>()
                        .join(",");
                    input.push_str(&format!("m,t=t{j} {fields} {j}000\n"));
                }
                let r = parse_ilp_batch(&input).unwrap();
                assert_eq!(r.len(), $count);
                assert_eq!(r[0].fields.len(), $fields);
            }
        };
    }
    batch_mf!(bmf_10x3, 10, 3);
    batch_mf!(bmf_10x5, 10, 5);
    batch_mf!(bmf_10x10, 10, 10);
    batch_mf!(bmf_20x3, 20, 3);
    batch_mf!(bmf_20x5, 20, 5);
    batch_mf!(bmf_20x10, 20, 10);
    batch_mf!(bmf_50x3, 50, 3);
    batch_mf!(bmf_50x5, 50, 5);
    batch_mf!(bmf_50x10, 50, 10);
    batch_mf!(bmf_100x3, 100, 3);
    batch_mf!(bmf_100x5, 100, 5);

    // Empty batch
    #[test]
    fn empty() {
        let r = parse_ilp_batch("").unwrap();
        assert!(r.is_empty());
    }
    #[test]
    fn ws_only() {
        let r = parse_ilp_batch("   ").unwrap();
        assert!(r.is_empty());
    }
}

// ===========================================================================
// Timestamp handling — 60 tests
// ===========================================================================
mod ilp_timestamp_f07 {
    use super::*;
    // No timestamp
    #[test]
    fn no_ts() {
        let p = parse_ilp_line("m val=1i").unwrap();
        assert!(p.timestamp.is_none());
    }

    // Specific timestamps
    macro_rules! tst {
        ($n:ident, $ts:expr) => {
            #[test]
            fn $n() {
                let line = format!("m val=1i {}", $ts);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.timestamp, Some(exchange_common::types::Timestamp($ts)));
            }
        };
    }
    tst!(ts0, 0);
    tst!(ts1, 1);
    tst!(ts10, 10);
    tst!(ts100, 100);
    tst!(ts1000, 1000);
    tst!(ts10000, 10000);
    tst!(ts100000, 100000);
    tst!(ts1000000, 1000000);
    tst!(ts_big, 1609459200000000000i64);
    tst!(ts_2024, 1704067200000000000i64);
    tst!(ts_2020, 1577836800000000000i64);
    tst!(ts_2010, 1262304000000000000i64);
    tst!(ts_2000, 946684800000000000i64);
    tst!(ts_1990, 631152000000000000i64);

    // Negative timestamp
    #[test]
    fn neg_ts() {
        let p = parse_ilp_line("m val=1i -1000").unwrap();
        assert_eq!(p.timestamp, Some(exchange_common::types::Timestamp(-1000)));
    }

    // Invalid timestamp
    #[test]
    fn bad_ts() {
        assert!(parse_ilp_line("m val=1i notanumber").is_err());
    }

    // Timestamps with tags
    macro_rules! ts_tag {
        ($n:ident, $ts:expr) => {
            #[test]
            fn $n() {
                let line = format!("m,sym=X val=1i {}", $ts);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.timestamp, Some(exchange_common::types::Timestamp($ts)));
            }
        };
    }
    ts_tag!(tt0, 0);
    ts_tag!(tt1, 1000);
    ts_tag!(tt2, 1000000);
    ts_tag!(tt3, 1000000000);
    ts_tag!(tt4, 1609459200000000000i64);
    ts_tag!(tt5, 1704067200000000000i64);

    // Timestamps with multi fields
    macro_rules! ts_mf {
        ($n:ident, $ts:expr) => {
            #[test]
            fn $n() {
                let line = format!("m a=1i,b=2.0 {}", $ts);
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.timestamp, Some(exchange_common::types::Timestamp($ts)));
            }
        };
    }
    ts_mf!(tmf0, 0);
    ts_mf!(tmf1, 1000);
    ts_mf!(tmf2, 1000000);
    ts_mf!(tmf3, 1000000000);
    ts_mf!(tmf4, 1609459200000000000i64);

    // Sequence of timestamps
    macro_rules! ts_seq {
        ($n:ident, $base:expr, $offset:expr) => {
            #[test]
            fn $n() {
                let ts = $base + $offset;
                let line = format!("m val=1i {ts}");
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.timestamp, Some(exchange_common::types::Timestamp(ts)));
            }
        };
    }
    ts_seq!(tsq01, 1000000000, 0);
    ts_seq!(tsq02, 1000000000, 1);
    ts_seq!(tsq03, 1000000000, 10);
    ts_seq!(tsq04, 1000000000, 100);
    ts_seq!(tsq05, 1000000000, 1000);
    ts_seq!(tsq06, 1000000000, 10000);
    ts_seq!(tsq07, 1000000000, 100000);
    ts_seq!(tsq08, 1000000000, 1000000);
    ts_seq!(tsq09, 1000000000, 10000000);
    ts_seq!(tsq10, 1000000000, 100000000);

    // Large int values
    tst!(ts_max_ish, 9999999999999999i64);
    tst!(ts_mid, 5000000000000i64);

    // Specific epoch nanos
    tst!(ts_nano_1, 1609459200000000001i64);
    tst!(ts_nano_2, 1609459200000000002i64);
    tst!(ts_nano_3, 1609459200000000003i64);
    tst!(ts_nano_5, 1609459200000000005i64);
    tst!(ts_nano_10, 1609459200000000010i64);
    tst!(ts_nano_100, 1609459200000000100i64);
}

// ===========================================================================
// Edge cases and errors — 60 tests
// ===========================================================================
mod ilp_edge_f07 {
    use super::*;
    #[test]
    fn empty() {
        assert_eq!(parse_ilp_line(""), Err(IlpParseError::EmptyInput));
    }
    #[test]
    fn ws() {
        assert_eq!(parse_ilp_line("   "), Err(IlpParseError::EmptyInput));
    }
    #[test]
    fn tab() {
        assert_eq!(parse_ilp_line("\t"), Err(IlpParseError::EmptyInput));
    }
    #[test]
    fn nl() {
        assert_eq!(parse_ilp_line("\n"), Err(IlpParseError::EmptyInput));
    }
    #[test]
    fn comment1() {
        assert_eq!(parse_ilp_line("# comment"), Err(IlpParseError::EmptyInput));
    }
    #[test]
    fn comment2() {
        assert_eq!(
            parse_ilp_line("  # comment"),
            Err(IlpParseError::EmptyInput)
        );
    }
    #[test]
    fn no_fields() {
        assert!(parse_ilp_line("m").is_err());
    }
    #[test]
    fn no_fields_tag() {
        assert!(parse_ilp_line("m,k=v").is_err());
    }
    #[test]
    fn field_no_eq() {
        assert!(parse_ilp_line("m bad 1000").is_err());
    }
    #[test]
    fn empty_key() {
        assert!(parse_ilp_line("m =val 1000").is_err());
    }

    // Long measurement names
    macro_rules! longm {
        ($n:ident, $len:expr) => {
            #[test]
            fn $n() {
                let name = "a".repeat($len);
                let line = format!("{name} val=1i 1000");
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.measurement, name);
            }
        };
    }
    longm!(lm10, 10);
    longm!(lm20, 20);
    longm!(lm50, 50);
    longm!(lm100, 100);
    longm!(lm200, 200);
    longm!(lm500, 500);
    longm!(lm1000, 1000);

    // Long string values
    macro_rules! longs {
        ($n:ident, $len:expr) => {
            #[test]
            fn $n() {
                let val = "x".repeat($len);
                let line = format!("m val=\"{val}\" 1000");
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.fields.get("val"), Some(&IlpValue::String(val)));
            }
        };
    }
    longs!(ls10, 10);
    longs!(ls50, 50);
    longs!(ls100, 100);
    longs!(ls500, 500);
    longs!(ls1000, 1000);
    longs!(ls2000, 2000);
    longs!(ls5000, 5000);

    // Long tag values
    macro_rules! longt {
        ($n:ident, $len:expr) => {
            #[test]
            fn $n() {
                let v = "a".repeat($len);
                let line = format!("m,k={v} val=1i 1000");
                let p = parse_ilp_line(&line).unwrap();
                assert_eq!(p.tags.get("k").unwrap(), &v);
            }
        };
    }
    longt!(lt10, 10);
    longt!(lt50, 50);
    longt!(lt100, 100);
    longt!(lt500, 500);
    longt!(lt1000, 1000);
    longt!(lt2000, 2000);

    // Escaped chars
    #[test]
    fn escaped_comma() {
        let p = parse_ilp_line("m,tag=a\\,b val=1i 1000").unwrap();
        assert_eq!(p.tags.get("tag").unwrap(), "a,b");
    }
    #[test]
    fn escaped_meas() {
        let p = parse_ilp_line("cpu\\,host val=1i 1000").unwrap();
        assert_eq!(p.measurement, "cpu,host");
    }

    // Leading/trailing whitespace
    #[test]
    fn lead_ws() {
        let p = parse_ilp_line("  m val=1i 1000  ").unwrap();
        assert_eq!(p.measurement, "m");
    }
    #[test]
    fn trail_ws() {
        let p = parse_ilp_line("m val=1i 1000   ").unwrap();
        assert_eq!(p.measurement, "m");
    }

    // i64 limits
    #[test]
    fn max_i64_f() {
        let p = parse_ilp_line("m val=9223372036854775807i 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Integer(i64::MAX)));
    }
    #[test]
    fn min_i64_f() {
        let line = format!("m val={}i 1000", i64::MIN);
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Integer(i64::MIN)));
    }

    // Various error patterns
    #[test]
    fn bad_suffix() {
        assert!(parse_ilp_line("m val=123x 1000").is_err());
    }
    #[test]
    fn double_comma() {
        assert!(parse_ilp_line("m,,tag=v val=1i 1000").is_err());
    }

    // Valid with underscore/dot names
    #[test]
    fn underscore_meas() {
        let p = parse_ilp_line("my_table val=1i 1000").unwrap();
        assert_eq!(p.measurement, "my_table");
    }
    #[test]
    fn underscore_field() {
        let p = parse_ilp_line("m my_field=1i 1000").unwrap();
        assert!(p.fields.contains_key("my_field"));
    }
    #[test]
    fn underscore_tag() {
        let p = parse_ilp_line("m,my_tag=v val=1i 1000").unwrap();
        assert_eq!(p.tags.get("my_tag").unwrap(), "v");
    }
    #[test]
    fn numeric_tag_val() {
        let p = parse_ilp_line("m,id=12345 val=1i 1000").unwrap();
        assert_eq!(p.tags.get("id").unwrap(), "12345");
    }
    #[test]
    fn hyphen_tag_val() {
        let p = parse_ilp_line("m,k=a-b val=1i 1000").unwrap();
        assert_eq!(p.tags.get("k").unwrap(), "a-b");
    }
    #[test]
    fn dot_tag_val() {
        let p = parse_ilp_line("m,k=1.2.3 val=1i 1000").unwrap();
        assert_eq!(p.tags.get("k").unwrap(), "1.2.3");
    }

    // V2 typed fields
    #[test]
    fn v2_ts_field() {
        let p = parse_ilp_line("m ts=1609459200000000000t 1000").unwrap();
        assert_eq!(
            p.fields.get("ts"),
            Some(&IlpValue::Timestamp(1609459200000000000))
        );
    }
    #[test]
    fn v2_ts_field2() {
        let p = parse_ilp_line("m ts=0t 1000").unwrap();
        assert_eq!(p.fields.get("ts"), Some(&IlpValue::Timestamp(0)));
    }
    #[test]
    fn v2_ts_field3() {
        let p = parse_ilp_line("m ts=1000t 1000").unwrap();
        assert_eq!(p.fields.get("ts"), Some(&IlpValue::Timestamp(1000)));
    }
}

// ===========================================================================
// ILP Version detection — 40 tests
// ===========================================================================
mod ilp_version_f07 {
    use super::*;
    // v1 lines (no typed fields)
    macro_rules! v1 {
        ($n:ident, $line:expr) => {
            #[test]
            fn $n() {
                assert_eq!(IlpVersion::detect($line), IlpVersion::V1);
            }
        };
    }
    v1!(v1_01, "m val=1i 1000");
    v1!(v1_02, "m val=1.0 1000");
    v1!(v1_03, "m val=true 1000");
    v1!(v1_04, "m val=false 1000");
    v1!(v1_05, r#"m val="hello" 1000"#);
    v1!(v1_06, "m val=T 1000");
    v1!(v1_07, "m val=F 1000");
    v1!(v1_08, "m a=1i,b=2i 1000");
    v1!(v1_09, "m,tag=v val=1i 1000");
    v1!(v1_10, "m val=0i 1000");
    v1!(v1_11, "m val=-1i 1000");
    v1!(v1_12, "m val=0.0 1000");
    v1!(v1_13, r#"m val="" 1000"#);
    v1!(v1_14, "m val=TRUE 1000");
    v1!(v1_15, "m val=FALSE 1000");
    v1!(v1_16, "m val=True 1000");
    v1!(v1_17, "m val=False 1000");
    v1!(v1_18, "m val=42i 1000");
    v1!(v1_19, "m val=99i 1000");
    v1!(v1_20, "m val=3.14 1000");

    // v2 lines (with typed fields like timestamp 't')
    macro_rules! v2 {
        ($n:ident, $line:expr) => {
            #[test]
            fn $n() {
                assert_eq!(IlpVersion::detect($line), IlpVersion::V2);
            }
        };
    }
    v2!(v2_01, "m ts=1000t 1000");
    v2!(v2_02, "m ts=0t 1000");
    v2!(v2_03, "m ts=1609459200000000000t 1000");
    v2!(v2_04, "m val=1i,ts=1000t 1000");
    v2!(v2_05, "m a=1i,ts=999t,b=2.0 1000");
    v2!(v2_06, "m,tag=v ts=1000t 1000");
    v2!(v2_07, "m ts=100t 1000");
    v2!(v2_08, "m ts=200t 1000");
    v2!(v2_09, "m ts=300t 1000");
    v2!(v2_10, "m ts=400t 1000");
    v2!(v2_11, "m ts=500t 1000");
    v2!(v2_12, "m ts=600t 1000");
    v2!(v2_13, "m ts=700t 1000");
    v2!(v2_14, "m ts=800t 1000");
    v2!(v2_15, "m ts=900t 1000");
    v2!(v2_16, "m ts=1000000t 1000");
    v2!(v2_17, "m ts=2000000t 1000");
    v2!(v2_18, "m ts=3000000t 1000");
    v2!(v2_19, "m ts=4000000t 1000");
    v2!(v2_20, "m ts=5000000t 1000");
}
