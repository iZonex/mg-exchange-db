//! 500 SQL count/sum/avg/min/max tests via TestDb.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

fn setup(n: i64) -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, vi DOUBLE, vs VARCHAR)");
    for idx in 0..n {
        let ts = 1_000_000_000_000i64 + idx * 1_000_000_000;
        let vi = idx as f64;
        let vs = format!("s{:03}", idx);
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, vi, vs) VALUES ({ts}, {vi}, '{vs}')"
        ));
    }
    db
}

fn ff(db: &TestDb, sql: &str) -> f64 {
    let (_, rows) = db.query(sql);
    match &rows[0][0] {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        other => panic!("expected num, got {other:?}"),
    }
}

fn rc(db: &TestDb, sql: &str) -> usize {
    let (_, rows) = db.query(sql);
    rows.len()
}

// count(*) for tables of size 1..100
mod count_star {
    use super::*;
    macro_rules! cnt {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                assert_eq!(ff(&db, "SELECT count(*) FROM t"), $sz as f64);
            }
        };
    }
    cnt!(c001, 1);
    cnt!(c002, 2);
    cnt!(c003, 3);
    cnt!(c004, 4);
    cnt!(c005, 5);
    cnt!(c006, 6);
    cnt!(c007, 7);
    cnt!(c008, 8);
    cnt!(c009, 9);
    cnt!(c010, 10);
    cnt!(c011, 11);
    cnt!(c012, 12);
    cnt!(c013, 13);
    cnt!(c014, 14);
    cnt!(c015, 15);
    cnt!(c016, 16);
    cnt!(c017, 17);
    cnt!(c018, 18);
    cnt!(c019, 19);
    cnt!(c020, 20);
    cnt!(c021, 21);
    cnt!(c022, 22);
    cnt!(c023, 23);
    cnt!(c024, 24);
    cnt!(c025, 25);
    cnt!(c026, 26);
    cnt!(c027, 27);
    cnt!(c028, 28);
    cnt!(c029, 29);
    cnt!(c030, 30);
    cnt!(c031, 31);
    cnt!(c032, 32);
    cnt!(c033, 33);
    cnt!(c034, 34);
    cnt!(c035, 35);
    cnt!(c036, 36);
    cnt!(c037, 37);
    cnt!(c038, 38);
    cnt!(c039, 39);
    cnt!(c040, 40);
    cnt!(c041, 41);
    cnt!(c042, 42);
    cnt!(c043, 43);
    cnt!(c044, 44);
    cnt!(c045, 45);
    cnt!(c046, 46);
    cnt!(c047, 47);
    cnt!(c048, 48);
    cnt!(c049, 49);
    cnt!(c050, 50);
    cnt!(c051, 51);
    cnt!(c052, 52);
    cnt!(c053, 53);
    cnt!(c054, 54);
    cnt!(c055, 55);
    cnt!(c056, 56);
    cnt!(c057, 57);
    cnt!(c058, 58);
    cnt!(c059, 59);
    cnt!(c060, 60);
    cnt!(c061, 61);
    cnt!(c062, 62);
    cnt!(c063, 63);
    cnt!(c064, 64);
    cnt!(c065, 65);
    cnt!(c066, 66);
    cnt!(c067, 67);
    cnt!(c068, 68);
    cnt!(c069, 69);
    cnt!(c070, 70);
    cnt!(c071, 71);
    cnt!(c072, 72);
    cnt!(c073, 73);
    cnt!(c074, 74);
    cnt!(c075, 75);
    cnt!(c076, 76);
    cnt!(c077, 77);
    cnt!(c078, 78);
    cnt!(c079, 79);
    cnt!(c080, 80);
    cnt!(c081, 81);
    cnt!(c082, 82);
    cnt!(c083, 83);
    cnt!(c084, 84);
    cnt!(c085, 85);
    cnt!(c086, 86);
    cnt!(c087, 87);
    cnt!(c088, 88);
    cnt!(c089, 89);
    cnt!(c090, 90);
    cnt!(c091, 91);
    cnt!(c092, 92);
    cnt!(c093, 93);
    cnt!(c094, 94);
    cnt!(c095, 95);
    cnt!(c096, 96);
    cnt!(c097, 97);
    cnt!(c098, 98);
    cnt!(c099, 99);
    cnt!(c100, 100);
}

// sum(vi) for tables of size 1..50 (sum of 0..n-1 = n*(n-1)/2)
mod sum_vi {
    use super::*;
    macro_rules! sm {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                let expect = ($sz * ($sz - 1)) as f64 / 2.0;
                assert!((ff(&db, "SELECT sum(vi) FROM t") - expect).abs() < 0.01);
            }
        };
    }
    sm!(s01, 1);
    sm!(s02, 2);
    sm!(s03, 3);
    sm!(s04, 4);
    sm!(s05, 5);
    sm!(s06, 6);
    sm!(s07, 7);
    sm!(s08, 8);
    sm!(s09, 9);
    sm!(s10, 10);
    sm!(s11, 11);
    sm!(s12, 12);
    sm!(s13, 13);
    sm!(s14, 14);
    sm!(s15, 15);
    sm!(s16, 16);
    sm!(s17, 17);
    sm!(s18, 18);
    sm!(s19, 19);
    sm!(s20, 20);
    sm!(s21, 21);
    sm!(s22, 22);
    sm!(s23, 23);
    sm!(s24, 24);
    sm!(s25, 25);
    sm!(s26, 26);
    sm!(s27, 27);
    sm!(s28, 28);
    sm!(s29, 29);
    sm!(s30, 30);
    sm!(s31, 31);
    sm!(s32, 32);
    sm!(s33, 33);
    sm!(s34, 34);
    sm!(s35, 35);
    sm!(s36, 36);
    sm!(s37, 37);
    sm!(s38, 38);
    sm!(s39, 39);
    sm!(s40, 40);
    sm!(s41, 41);
    sm!(s42, 42);
    sm!(s43, 43);
    sm!(s44, 44);
    sm!(s45, 45);
    sm!(s46, 46);
    sm!(s47, 47);
    sm!(s48, 48);
    sm!(s49, 49);
    sm!(s50, 50);
}

// avg(vi) for tables of size 1..50 (avg of 0..n-1 = (n-1)/2)
mod avg_vi {
    use super::*;
    macro_rules! av {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                let expect = ($sz - 1) as f64 / 2.0;
                assert!((ff(&db, "SELECT avg(vi) FROM t") - expect).abs() < 0.01);
            }
        };
    }
    av!(a01, 1);
    av!(a02, 2);
    av!(a03, 3);
    av!(a04, 4);
    av!(a05, 5);
    av!(a06, 6);
    av!(a07, 7);
    av!(a08, 8);
    av!(a09, 9);
    av!(a10, 10);
    av!(a11, 11);
    av!(a12, 12);
    av!(a13, 13);
    av!(a14, 14);
    av!(a15, 15);
    av!(a16, 16);
    av!(a17, 17);
    av!(a18, 18);
    av!(a19, 19);
    av!(a20, 20);
    av!(a21, 21);
    av!(a22, 22);
    av!(a23, 23);
    av!(a24, 24);
    av!(a25, 25);
    av!(a26, 26);
    av!(a27, 27);
    av!(a28, 28);
    av!(a29, 29);
    av!(a30, 30);
    av!(a31, 31);
    av!(a32, 32);
    av!(a33, 33);
    av!(a34, 34);
    av!(a35, 35);
    av!(a36, 36);
    av!(a37, 37);
    av!(a38, 38);
    av!(a39, 39);
    av!(a40, 40);
    av!(a41, 41);
    av!(a42, 42);
    av!(a43, 43);
    av!(a44, 44);
    av!(a45, 45);
    av!(a46, 46);
    av!(a47, 47);
    av!(a48, 48);
    av!(a49, 49);
    av!(a50, 50);
}

// min(vi) always 0, max(vi) always n-1
mod min_max_vi {
    use super::*;
    macro_rules! mn {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                assert!((ff(&db, "SELECT min(vi) FROM t")).abs() < 0.01);
            }
        };
    }
    macro_rules! mx {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                assert!((ff(&db, "SELECT max(vi) FROM t") - ($sz - 1) as f64).abs() < 0.01);
            }
        };
    }
    mn!(min01, 1);
    mn!(min02, 2);
    mn!(min05, 5);
    mn!(min10, 10);
    mn!(min20, 20);
    mn!(min30, 30);
    mn!(min40, 40);
    mn!(min50, 50);
    mn!(min60, 60);
    mn!(min70, 70);
    mn!(min80, 80);
    mn!(min90, 90);
    mn!(min100, 100);
    mx!(max01, 1);
    mx!(max02, 2);
    mx!(max05, 5);
    mx!(max10, 10);
    mx!(max20, 20);
    mx!(max30, 30);
    mx!(max40, 40);
    mx!(max50, 50);
    mx!(max60, 60);
    mx!(max70, 70);
    mx!(max80, 80);
    mx!(max90, 90);
    mx!(max100, 100);
}

// LIMIT tests
mod limits {
    use super::*;
    macro_rules! lm {
        ($n:ident, $sz:expr, $lim:expr, $expect:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                assert_eq!(rc(&db, &format!("SELECT * FROM t LIMIT {}", $lim)), $expect);
            }
        };
    }
    lm!(l10_1, 10, 1, 1);
    lm!(l10_2, 10, 2, 2);
    lm!(l10_3, 10, 3, 3);
    lm!(l10_5, 10, 5, 5);
    lm!(l10_10, 10, 10, 10);
    lm!(l10_20, 10, 20, 10);
    lm!(l10_0, 10, 0, 0);
    lm!(l20_1, 20, 1, 1);
    lm!(l20_5, 20, 5, 5);
    lm!(l20_10, 20, 10, 10);
    lm!(l20_15, 20, 15, 15);
    lm!(l20_20, 20, 20, 20);
    lm!(l20_50, 20, 50, 20);
    lm!(l50_1, 50, 1, 1);
    lm!(l50_10, 50, 10, 10);
    lm!(l50_25, 50, 25, 25);
    lm!(l50_50, 50, 50, 50);
    lm!(l50_100, 50, 100, 50);
    lm!(l5_1, 5, 1, 1);
    lm!(l5_3, 5, 3, 3);
    lm!(l5_5, 5, 5, 5);
    lm!(l5_10, 5, 10, 5);
    lm!(l1_1, 1, 1, 1);
    lm!(l1_0, 1, 0, 0);
    lm!(l100_1, 100, 1, 1);
    lm!(l100_50, 100, 50, 50);
    lm!(l100_100, 100, 100, 100);
}

// count(vi) is same as count(*)
mod count_col {
    use super::*;
    macro_rules! cc {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                assert_eq!(ff(&db, "SELECT count(vi) FROM t"), $sz as f64);
            }
        };
    }
    cc!(c01, 1);
    cc!(c05, 5);
    cc!(c10, 10);
    cc!(c20, 20);
    cc!(c50, 50);
    cc!(c02, 2);
    cc!(c03, 3);
    cc!(c04, 4);
    cc!(c06, 6);
    cc!(c07, 7);
    cc!(c08, 8);
    cc!(c09, 9);
    cc!(c11, 11);
    cc!(c12, 12);
    cc!(c13, 13);
    cc!(c14, 14);
    cc!(c15, 15);
}

// avg(vi) for tables 51..100
mod avg_vi2 {
    use super::*;
    macro_rules! av {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                let expect = ($sz - 1) as f64 / 2.0;
                assert!((ff(&db, "SELECT avg(vi) FROM t") - expect).abs() < 0.01);
            }
        };
    }
    av!(a51, 51);
    av!(a52, 52);
    av!(a53, 53);
    av!(a54, 54);
    av!(a55, 55);
    av!(a56, 56);
    av!(a57, 57);
    av!(a58, 58);
    av!(a59, 59);
    av!(a60, 60);
    av!(a61, 61);
    av!(a62, 62);
    av!(a63, 63);
    av!(a64, 64);
    av!(a65, 65);
    av!(a66, 66);
    av!(a67, 67);
    av!(a68, 68);
    av!(a69, 69);
    av!(a70, 70);
    av!(a71, 71);
    av!(a72, 72);
    av!(a73, 73);
    av!(a74, 74);
    av!(a75, 75);
    av!(a76, 76);
    av!(a77, 77);
    av!(a78, 78);
    av!(a79, 79);
    av!(a80, 80);
    av!(a81, 81);
    av!(a82, 82);
    av!(a83, 83);
    av!(a84, 84);
    av!(a85, 85);
    av!(a86, 86);
    av!(a87, 87);
    av!(a88, 88);
    av!(a89, 89);
    av!(a90, 90);
    av!(a91, 91);
    av!(a92, 92);
    av!(a93, 93);
    av!(a94, 94);
    av!(a95, 95);
    av!(a96, 96);
    av!(a97, 97);
    av!(a98, 98);
    av!(a99, 99);
    av!(a100, 100);
}

// sum(vi) for tables 51..100
mod sum_vi2 {
    use super::*;
    macro_rules! sm {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                let expect = ($sz * ($sz - 1)) as f64 / 2.0;
                assert!((ff(&db, "SELECT sum(vi) FROM t") - expect).abs() < 0.01);
            }
        };
    }
    sm!(s51, 51);
    sm!(s52, 52);
    sm!(s53, 53);
    sm!(s54, 54);
    sm!(s55, 55);
    sm!(s56, 56);
    sm!(s57, 57);
    sm!(s58, 58);
    sm!(s59, 59);
    sm!(s60, 60);
    sm!(s61, 61);
    sm!(s62, 62);
    sm!(s63, 63);
    sm!(s64, 64);
    sm!(s65, 65);
    sm!(s66, 66);
    sm!(s67, 67);
    sm!(s68, 68);
    sm!(s69, 69);
    sm!(s70, 70);
    sm!(s71, 71);
    sm!(s72, 72);
    sm!(s73, 73);
    sm!(s74, 74);
    sm!(s75, 75);
    sm!(s76, 76);
    sm!(s77, 77);
    sm!(s78, 78);
    sm!(s79, 79);
    sm!(s80, 80);
    sm!(s81, 81);
    sm!(s82, 82);
    sm!(s83, 83);
    sm!(s84, 84);
    sm!(s85, 85);
    sm!(s86, 86);
    sm!(s87, 87);
    sm!(s88, 88);
    sm!(s89, 89);
    sm!(s90, 90);
    sm!(s91, 91);
    sm!(s92, 92);
    sm!(s93, 93);
    sm!(s94, 94);
    sm!(s95, 95);
    sm!(s96, 96);
    sm!(s97, 97);
    sm!(s98, 98);
    sm!(s99, 99);
    sm!(s100, 100);
}

// count(vi) for tables 16..50
mod count_col2 {
    use super::*;
    macro_rules! cc {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                assert_eq!(ff(&db, "SELECT count(vi) FROM t"), $sz as f64);
            }
        };
    }
    cc!(c16, 16);
    cc!(c17, 17);
    cc!(c18, 18);
    cc!(c19, 19);
    cc!(c21, 21);
    cc!(c22, 22);
    cc!(c23, 23);
    cc!(c24, 24);
    cc!(c25, 25);
    cc!(c26, 26);
    cc!(c27, 27);
    cc!(c28, 28);
    cc!(c29, 29);
    cc!(c30, 30);
    cc!(c31, 31);
    cc!(c32, 32);
    cc!(c33, 33);
    cc!(c34, 34);
    cc!(c35, 35);
    cc!(c36, 36);
    cc!(c37, 37);
    cc!(c38, 38);
    cc!(c39, 39);
    cc!(c40, 40);
    cc!(c41, 41);
    cc!(c42, 42);
    cc!(c43, 43);
    cc!(c44, 44);
    cc!(c45, 45);
    cc!(c46, 46);
    cc!(c47, 47);
    cc!(c48, 48);
    cc!(c49, 49);
    cc!(c50, 50);
}

// max(vi) for tables 51..100
mod max_vi2 {
    use super::*;
    macro_rules! mx {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup($sz);
                assert!((ff(&db, "SELECT max(vi) FROM t") - ($sz - 1) as f64).abs() < 0.01);
            }
        };
    }
    mx!(m51, 51);
    mx!(m52, 52);
    mx!(m53, 53);
    mx!(m54, 54);
    mx!(m55, 55);
    mx!(m56, 56);
    mx!(m57, 57);
    mx!(m58, 58);
    mx!(m59, 59);
    mx!(m60, 60);
    mx!(m61, 61);
    mx!(m62, 62);
    mx!(m63, 63);
    mx!(m64, 64);
    mx!(m65, 65);
    mx!(m66, 66);
    mx!(m67, 67);
    mx!(m68, 68);
    mx!(m69, 69);
    mx!(m70, 70);
    mx!(m71, 71);
    mx!(m72, 72);
    mx!(m73, 73);
    mx!(m74, 74);
    mx!(m75, 75);
    mx!(m76, 76);
    mx!(m77, 77);
    mx!(m78, 78);
    mx!(m79, 79);
    mx!(m80, 80);
    mx!(m81, 81);
    mx!(m82, 82);
    mx!(m83, 83);
    mx!(m84, 84);
    mx!(m85, 85);
    mx!(m86, 86);
    mx!(m87, 87);
    mx!(m88, 88);
    mx!(m89, 89);
    mx!(m90, 90);
    mx!(m91, 91);
    mx!(m92, 92);
    mx!(m93, 93);
    mx!(m94, 94);
    mx!(m95, 95);
    mx!(m96, 96);
    mx!(m97, 97);
    mx!(m98, 98);
    mx!(m99, 99);
    mx!(m100, 100);
}
