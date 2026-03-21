//! 500 column write/read tests.

use exchange_common::types::ColumnType;
use exchange_core::column::{
    FixedColumnReader, FixedColumnWriter, VarColumnReader, VarColumnWriter,
};
use tempfile::tempdir;

// i64 values -50..50
mod fixed_i64_range {
    use super::*;
    macro_rules! i64t {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
                    w.append_i64($val).unwrap();
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
                assert_eq!(r.row_count(), 1);
                assert_eq!(r.read_i64(0), $val);
            }
        };
    }
    i64t!(n50, -50);
    i64t!(n49, -49);
    i64t!(n48, -48);
    i64t!(n47, -47);
    i64t!(n46, -46);
    i64t!(n45, -45);
    i64t!(n44, -44);
    i64t!(n43, -43);
    i64t!(n42, -42);
    i64t!(n41, -41);
    i64t!(n40, -40);
    i64t!(n39, -39);
    i64t!(n38, -38);
    i64t!(n37, -37);
    i64t!(n36, -36);
    i64t!(n35, -35);
    i64t!(n34, -34);
    i64t!(n33, -33);
    i64t!(n32, -32);
    i64t!(n31, -31);
    i64t!(n30, -30);
    i64t!(n29, -29);
    i64t!(n28, -28);
    i64t!(n27, -27);
    i64t!(n26, -26);
    i64t!(n25, -25);
    i64t!(n24, -24);
    i64t!(n23, -23);
    i64t!(n22, -22);
    i64t!(n21, -21);
    i64t!(n20, -20);
    i64t!(n19, -19);
    i64t!(n18, -18);
    i64t!(n17, -17);
    i64t!(n16, -16);
    i64t!(n15, -15);
    i64t!(n14, -14);
    i64t!(n13, -13);
    i64t!(n12, -12);
    i64t!(n11, -11);
    i64t!(n10, -10);
    i64t!(n09, -9);
    i64t!(n08, -8);
    i64t!(n07, -7);
    i64t!(n06, -6);
    i64t!(n05, -5);
    i64t!(n04, -4);
    i64t!(n03, -3);
    i64t!(n02, -2);
    i64t!(n01, -1);
    i64t!(p00, 0);
    i64t!(p01, 1);
    i64t!(p02, 2);
    i64t!(p03, 3);
    i64t!(p04, 4);
    i64t!(p05, 5);
    i64t!(p06, 6);
    i64t!(p07, 7);
    i64t!(p08, 8);
    i64t!(p09, 9);
    i64t!(p10, 10);
    i64t!(p11, 11);
    i64t!(p12, 12);
    i64t!(p13, 13);
    i64t!(p14, 14);
    i64t!(p15, 15);
    i64t!(p16, 16);
    i64t!(p17, 17);
    i64t!(p18, 18);
    i64t!(p19, 19);
    i64t!(p20, 20);
    i64t!(p21, 21);
    i64t!(p22, 22);
    i64t!(p23, 23);
    i64t!(p24, 24);
    i64t!(p25, 25);
    i64t!(p26, 26);
    i64t!(p27, 27);
    i64t!(p28, 28);
    i64t!(p29, 29);
    i64t!(p30, 30);
    i64t!(p31, 31);
    i64t!(p32, 32);
    i64t!(p33, 33);
    i64t!(p34, 34);
    i64t!(p35, 35);
    i64t!(p36, 36);
    i64t!(p37, 37);
    i64t!(p38, 38);
    i64t!(p39, 39);
    i64t!(p40, 40);
    i64t!(p41, 41);
    i64t!(p42, 42);
    i64t!(p43, 43);
    i64t!(p44, 44);
    i64t!(p45, 45);
    i64t!(p46, 46);
    i64t!(p47, 47);
    i64t!(p48, 48);
    i64t!(p49, 49);
    i64t!(p50, 50);
}

// f64 values -10.0 to 10.0 step 0.5
mod fixed_f64_range {
    use super::*;
    macro_rules! f64t {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
                    w.append_f64($val).unwrap();
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
                assert_eq!(r.row_count(), 1);
                assert!((r.read_f64(0) - $val).abs() < 0.001);
            }
        };
    }
    f64t!(n10_0, -10.0);
    f64t!(n9_5, -9.5);
    f64t!(n9_0, -9.0);
    f64t!(n8_5, -8.5);
    f64t!(n8_0, -8.0);
    f64t!(n7_5, -7.5);
    f64t!(n7_0, -7.0);
    f64t!(n6_5, -6.5);
    f64t!(n6_0, -6.0);
    f64t!(n5_5, -5.5);
    f64t!(n5_0, -5.0);
    f64t!(n4_5, -4.5);
    f64t!(n4_0, -4.0);
    f64t!(n3_5, -3.5);
    f64t!(n3_0, -3.0);
    f64t!(n2_5, -2.5);
    f64t!(n2_0, -2.0);
    f64t!(n1_5, -1.5);
    f64t!(n1_0, -1.0);
    f64t!(n0_5, -0.5);
    f64t!(p0_0, 0.0);
    f64t!(p0_5, 0.5);
    f64t!(p1_0, 1.0);
    f64t!(p1_5, 1.5);
    f64t!(p2_0, 2.0);
    f64t!(p2_5, 2.5);
    f64t!(p3_0, 3.0);
    f64t!(p3_5, 3.5);
    f64t!(p4_0, 4.0);
    f64t!(p4_5, 4.5);
    f64t!(p5_0, 5.0);
    f64t!(p5_5, 5.5);
    f64t!(p6_0, 6.0);
    f64t!(p6_5, 6.5);
    f64t!(p7_0, 7.0);
    f64t!(p7_5, 7.5);
    f64t!(p8_0, 8.0);
    f64t!(p8_5, 8.5);
    f64t!(p9_0, 9.0);
    f64t!(p9_5, 9.5);
    f64t!(p10_0, 10.0);
}

// var column strings of length 1..50
mod var_col_strings {
    use super::*;
    macro_rules! vst {
        ($n:ident, $len:expr) => {
            #[test]
            fn $n() {
                let s = "x".repeat($len);
                let dir = tempdir().unwrap();
                let dp = dir.path().join("c.d");
                let ip = dir.path().join("c.i");
                {
                    let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
                    w.append_str(&s).unwrap();
                    w.flush().unwrap();
                }
                let r = VarColumnReader::open(&dp, &ip).unwrap();
                assert_eq!(r.row_count(), 1);
                assert_eq!(r.read_str(0), s);
            }
        };
    }
    vst!(s01, 1);
    vst!(s02, 2);
    vst!(s03, 3);
    vst!(s04, 4);
    vst!(s05, 5);
    vst!(s06, 6);
    vst!(s07, 7);
    vst!(s08, 8);
    vst!(s09, 9);
    vst!(s10, 10);
    vst!(s11, 11);
    vst!(s12, 12);
    vst!(s13, 13);
    vst!(s14, 14);
    vst!(s15, 15);
    vst!(s16, 16);
    vst!(s17, 17);
    vst!(s18, 18);
    vst!(s19, 19);
    vst!(s20, 20);
    vst!(s21, 21);
    vst!(s22, 22);
    vst!(s23, 23);
    vst!(s24, 24);
    vst!(s25, 25);
    vst!(s26, 26);
    vst!(s27, 27);
    vst!(s28, 28);
    vst!(s29, 29);
    vst!(s30, 30);
    vst!(s31, 31);
    vst!(s32, 32);
    vst!(s33, 33);
    vst!(s34, 34);
    vst!(s35, 35);
    vst!(s36, 36);
    vst!(s37, 37);
    vst!(s38, 38);
    vst!(s39, 39);
    vst!(s40, 40);
    vst!(s41, 41);
    vst!(s42, 42);
    vst!(s43, 43);
    vst!(s44, 44);
    vst!(s45, 45);
    vst!(s46, 46);
    vst!(s47, 47);
    vst!(s48, 48);
    vst!(s49, 49);
    vst!(s50, 50);
}

// i64 multi-value writes
mod i64_multi {
    use super::*;
    macro_rules! i64m {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
                    for i in 0..$count as i64 {
                        w.append_i64(i * 3).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
                assert_eq!(r.row_count(), $count);
                for i in 0..$count as u64 {
                    assert_eq!(r.read_i64(i), i as i64 * 3);
                }
            }
        };
    }
    i64m!(m01, 1);
    i64m!(m02, 2);
    i64m!(m03, 3);
    i64m!(m05, 5);
    i64m!(m10, 10);
    i64m!(m15, 15);
    i64m!(m20, 20);
    i64m!(m25, 25);
    i64m!(m30, 30);
    i64m!(m40, 40);
    i64m!(m50, 50);
    i64m!(m75, 75);
    i64m!(m100, 100);
    i64m!(m150, 150);
    i64m!(m200, 200);
    i64m!(m250, 250);
    i64m!(m300, 300);
    i64m!(m400, 400);
    i64m!(m500, 500);
    i64m!(m1000, 1000);
}

// f64 multi-value writes
mod f64_multi {
    use super::*;
    macro_rules! f64m {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
                    for i in 0..$count {
                        w.append_f64(i as f64 * 0.1).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
                assert_eq!(r.row_count(), $count);
            }
        };
    }
    f64m!(m01, 1);
    f64m!(m02, 2);
    f64m!(m03, 3);
    f64m!(m05, 5);
    f64m!(m10, 10);
    f64m!(m15, 15);
    f64m!(m20, 20);
    f64m!(m25, 25);
    f64m!(m30, 30);
    f64m!(m40, 40);
    f64m!(m50, 50);
    f64m!(m75, 75);
    f64m!(m100, 100);
    f64m!(m150, 150);
    f64m!(m200, 200);
    f64m!(m250, 250);
    f64m!(m300, 300);
    f64m!(m400, 400);
    f64m!(m500, 500);
    f64m!(m1000, 1000);
}

// var column multi-value writes
mod var_multi {
    use super::*;
    macro_rules! vm {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let dp = dir.path().join("c.d");
                let ip = dir.path().join("c.i");
                {
                    let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
                    for i in 0..$count {
                        w.append_str(&format!("v{i}")).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = VarColumnReader::open(&dp, &ip).unwrap();
                assert_eq!(r.row_count(), $count);
                assert_eq!(r.read_str(0), "v0");
            }
        };
    }
    vm!(m01, 1);
    vm!(m02, 2);
    vm!(m03, 3);
    vm!(m05, 5);
    vm!(m10, 10);
    vm!(m15, 15);
    vm!(m20, 20);
    vm!(m25, 25);
    vm!(m30, 30);
    vm!(m40, 40);
    vm!(m50, 50);
    vm!(m75, 75);
    vm!(m100, 100);
    vm!(m150, 150);
    vm!(m200, 200);
}

// i64 special values
mod i64_special {
    use super::*;
    macro_rules! sp {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
                    w.append_i64($val).unwrap();
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
                assert_eq!(r.read_i64(0), $val);
            }
        };
    }
    sp!(min_val, i64::MIN);
    sp!(max_val, i64::MAX);
    sp!(zero, 0);
    sp!(one, 1);
    sp!(neg_one, -1);
    sp!(k100, 100);
    sp!(k1000, 1000);
    sp!(k10000, 10000);
    sp!(k100000, 100000);
    sp!(k_neg100, -100);
    sp!(k_neg1000, -1000);
    sp!(k_neg10000, -10000);
    sp!(million, 1_000_000);
    sp!(neg_million, -1_000_000);
    sp!(billion, 1_000_000_000);
    sp!(neg_billion, -1_000_000_000);
}

// empty columns
mod empty_cols {
    use super::*;
    #[test]
    fn i64_empty() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 0);
    }
    #[test]
    fn f64_empty() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
        assert_eq!(r.row_count(), 0);
    }
    #[test]
    fn var_empty() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("c.d");
        let ip = dir.path().join("c.i");
        {
            let w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 0);
    }
}

// i64 constant value fills
mod i64_const {
    use super::*;
    macro_rules! ic {
        ($n:ident, $val:expr, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
                    for _ in 0..$count {
                        w.append_i64($val).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
                assert_eq!(r.row_count(), $count);
                for i in 0..$count as u64 {
                    assert_eq!(r.read_i64(i), $val);
                }
            }
        };
    }
    ic!(z10, 0, 10);
    ic!(z50, 0, 50);
    ic!(z100, 0, 100);
    ic!(o10, 1, 10);
    ic!(o50, 1, 50);
    ic!(o100, 1, 100);
    ic!(f10, 42, 10);
    ic!(f50, 42, 50);
    ic!(f100, 42, 100);
    ic!(n10, -1, 10);
    ic!(n50, -1, 50);
    ic!(n100, -1, 100);
    ic!(h10, 999, 10);
    ic!(h50, 999, 50);
    ic!(h100, 999, 100);
    ic!(max10, i64::MAX, 10);
    ic!(min10, i64::MIN, 10);
}

// var column with specific strings
mod var_specific {
    use super::*;
    macro_rules! vs {
        ($n:ident, $s:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let dp = dir.path().join("c.d");
                let ip = dir.path().join("c.i");
                {
                    let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
                    w.append_str($s).unwrap();
                    w.flush().unwrap();
                }
                let r = VarColumnReader::open(&dp, &ip).unwrap();
                assert_eq!(r.read_str(0), $s);
            }
        };
    }
    vs!(empty, "");
    vs!(a, "a");
    vs!(hello, "hello");
    vs!(world, "world");
    vs!(space, " ");
    vs!(tab, "\t");
    vs!(nl, "\n");
    vs!(special, "!@#$%");
    vs!(digits, "0123456789");
    vs!(alpha, "abcdefghijklmnopqrstuvwxyz");
    vs!(mixed, "Hello, World!");
    vs!(path, "/usr/local/bin");
    vs!(long100, &"a".repeat(100));
    vs!(long500, &"b".repeat(500));
    vs!(long1000, &"c".repeat(1000));
    vs!(long5000, &"d".repeat(5000));
}

// i64 ascending sequences of different sizes
mod i64_asc_seq {
    use super::*;
    macro_rules! aseq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
                    for i in 0..$count as i64 {
                        w.append_i64(i).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
                assert_eq!(r.row_count(), $count);
                assert_eq!(r.read_i64(0), 0);
                assert_eq!(r.read_i64($count - 1), ($count - 1) as i64);
            }
        };
    }
    aseq!(a1, 1);
    aseq!(a2, 2);
    aseq!(a3, 3);
    aseq!(a4, 4);
    aseq!(a5, 5);
    aseq!(a6, 6);
    aseq!(a7, 7);
    aseq!(a8, 8);
    aseq!(a9, 9);
    aseq!(a10, 10);
    aseq!(a11, 11);
    aseq!(a12, 12);
    aseq!(a13, 13);
    aseq!(a14, 14);
    aseq!(a15, 15);
    aseq!(a16, 16);
    aseq!(a17, 17);
    aseq!(a18, 18);
    aseq!(a19, 19);
    aseq!(a20, 20);
    aseq!(a25, 25);
    aseq!(a30, 30);
    aseq!(a35, 35);
    aseq!(a40, 40);
    aseq!(a45, 45);
    aseq!(a50, 50);
    aseq!(a60, 60);
    aseq!(a70, 70);
    aseq!(a80, 80);
    aseq!(a90, 90);
    aseq!(a100, 100);
    aseq!(a200, 200);
    aseq!(a500, 500);
}

// f64 ascending sequences
mod f64_asc_seq {
    use super::*;
    macro_rules! aseq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
                    for i in 0..$count {
                        w.append_f64(i as f64 * 0.5).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
                assert_eq!(r.row_count(), $count);
                assert!((r.read_f64(0)).abs() < 0.001);
            }
        };
    }
    aseq!(a1, 1);
    aseq!(a2, 2);
    aseq!(a5, 5);
    aseq!(a10, 10);
    aseq!(a20, 20);
    aseq!(a50, 50);
    aseq!(a100, 100);
    aseq!(a200, 200);
    aseq!(a500, 500);
}

// var column sequential strings
mod var_seq {
    use super::*;
    macro_rules! vseq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let dp = dir.path().join("c.d");
                let ip = dir.path().join("c.i");
                {
                    let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
                    for i in 0..$count {
                        w.append_str(&format!("item_{i:04}")).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = VarColumnReader::open(&dp, &ip).unwrap();
                assert_eq!(r.row_count(), $count);
                assert_eq!(r.read_str(0), "item_0000");
                if $count > 1 {
                    assert_eq!(r.read_str($count - 1), format!("item_{:04}", $count - 1));
                }
            }
        };
    }
    vseq!(v1, 1);
    vseq!(v2, 2);
    vseq!(v5, 5);
    vseq!(v10, 10);
    vseq!(v20, 20);
    vseq!(v50, 50);
    vseq!(v100, 100);
    vseq!(v200, 200);
    vseq!(v500, 500);
}

// i64 descending sequences
mod i64_desc_seq {
    use super::*;
    macro_rules! dseq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
                    for i in (0..$count as i64).rev() {
                        w.append_i64(i).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
                assert_eq!(r.row_count(), $count);
                assert_eq!(r.read_i64(0), ($count - 1) as i64);
                assert_eq!(r.read_i64($count - 1), 0);
            }
        };
    }
    dseq!(d5, 5);
    dseq!(d10, 10);
    dseq!(d20, 20);
    dseq!(d50, 50);
    dseq!(d100, 100);
    dseq!(d200, 200);
    dseq!(d500, 500);
}

// f64 constant fill
mod f64_const {
    use super::*;
    macro_rules! fc {
        ($n:ident, $val:expr, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
                    for _ in 0..$count {
                        w.append_f64($val).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
                assert_eq!(r.row_count(), $count);
                for i in 0..$count as u64 {
                    assert!((r.read_f64(i) - $val).abs() < 0.001);
                }
            }
        };
    }
    fc!(z10, 0.0, 10);
    fc!(z50, 0.0, 50);
    fc!(z100, 0.0, 100);
    fc!(pi10, 3.14159, 10);
    fc!(pi50, 3.14159, 50);
    fc!(pi100, 3.14159, 100);
    fc!(neg10, -1.5, 10);
    fc!(neg50, -1.5, 50);
    fc!(neg100, -1.5, 100);
    fc!(big10, 99999.99, 10);
    fc!(big50, 99999.99, 50);
}

// var column empty strings
mod var_empty_strings {
    use super::*;
    macro_rules! ve {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let dp = dir.path().join("c.d");
                let ip = dir.path().join("c.i");
                {
                    let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
                    for _ in 0..$count {
                        w.append_str("").unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = VarColumnReader::open(&dp, &ip).unwrap();
                assert_eq!(r.row_count(), $count);
                for i in 0..$count as u64 {
                    assert_eq!(r.read_str(i), "");
                }
            }
        };
    }
    ve!(e1, 1);
    ve!(e5, 5);
    ve!(e10, 10);
    ve!(e50, 50);
    ve!(e100, 100);
}
