//! 500 column write/read, WAL, compression, symbol map tests.

use exchange_common::types::ColumnType;
use exchange_core::column::{
    FixedColumnReader, FixedColumnWriter, VarColumnReader, VarColumnWriter,
};
use tempfile::tempdir;

// ===========================================================================
// i64 write/read — range 51..150 (100 tests)
// ===========================================================================
mod fixed_i64_ext {
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
    i64t!(p51, 51);
    i64t!(p52, 52);
    i64t!(p53, 53);
    i64t!(p54, 54);
    i64t!(p55, 55);
    i64t!(p56, 56);
    i64t!(p57, 57);
    i64t!(p58, 58);
    i64t!(p59, 59);
    i64t!(p60, 60);
    i64t!(p61, 61);
    i64t!(p62, 62);
    i64t!(p63, 63);
    i64t!(p64, 64);
    i64t!(p65, 65);
    i64t!(p66, 66);
    i64t!(p67, 67);
    i64t!(p68, 68);
    i64t!(p69, 69);
    i64t!(p70, 70);
    i64t!(p71, 71);
    i64t!(p72, 72);
    i64t!(p73, 73);
    i64t!(p74, 74);
    i64t!(p75, 75);
    i64t!(p76, 76);
    i64t!(p77, 77);
    i64t!(p78, 78);
    i64t!(p79, 79);
    i64t!(p80, 80);
    i64t!(p81, 81);
    i64t!(p82, 82);
    i64t!(p83, 83);
    i64t!(p84, 84);
    i64t!(p85, 85);
    i64t!(p86, 86);
    i64t!(p87, 87);
    i64t!(p88, 88);
    i64t!(p89, 89);
    i64t!(p90, 90);
    i64t!(p91, 91);
    i64t!(p92, 92);
    i64t!(p93, 93);
    i64t!(p94, 94);
    i64t!(p95, 95);
    i64t!(p96, 96);
    i64t!(p97, 97);
    i64t!(p98, 98);
    i64t!(p99, 99);
    i64t!(p100, 100);
    i64t!(n51, -51);
    i64t!(n52, -52);
    i64t!(n53, -53);
    i64t!(n54, -54);
    i64t!(n55, -55);
    i64t!(n56, -56);
    i64t!(n57, -57);
    i64t!(n58, -58);
    i64t!(n59, -59);
    i64t!(n60, -60);
    i64t!(n61, -61);
    i64t!(n62, -62);
    i64t!(n63, -63);
    i64t!(n64, -64);
    i64t!(n65, -65);
    i64t!(n66, -66);
    i64t!(n67, -67);
    i64t!(n68, -68);
    i64t!(n69, -69);
    i64t!(n70, -70);
    i64t!(n71, -71);
    i64t!(n72, -72);
    i64t!(n73, -73);
    i64t!(n74, -74);
    i64t!(n75, -75);
    i64t!(n76, -76);
    i64t!(n77, -77);
    i64t!(n78, -78);
    i64t!(n79, -79);
    i64t!(n80, -80);
    i64t!(n81, -81);
    i64t!(n82, -82);
    i64t!(n83, -83);
    i64t!(n84, -84);
    i64t!(n85, -85);
    i64t!(n86, -86);
    i64t!(n87, -87);
    i64t!(n88, -88);
    i64t!(n89, -89);
    i64t!(n90, -90);
    i64t!(n91, -91);
    i64t!(n92, -92);
    i64t!(n93, -93);
    i64t!(n94, -94);
    i64t!(n95, -95);
    i64t!(n96, -96);
    i64t!(n97, -97);
    i64t!(n98, -98);
    i64t!(n99, -99);
    i64t!(n100, -100);
}

// ===========================================================================
// f64 write/read — specific values (50 tests)
// ===========================================================================
mod fixed_f64_ext {
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
    f64t!(p11_0, 11.0);
    f64t!(p11_5, 11.5);
    f64t!(p12_0, 12.0);
    f64t!(p12_5, 12.5);
    f64t!(p13_0, 13.0);
    f64t!(p13_5, 13.5);
    f64t!(p14_0, 14.0);
    f64t!(p14_5, 14.5);
    f64t!(p15_0, 15.0);
    f64t!(p15_5, 15.5);
    f64t!(p16_0, 16.0);
    f64t!(p17_0, 17.0);
    f64t!(p18_0, 18.0);
    f64t!(p19_0, 19.0);
    f64t!(p20_0, 20.0);
    f64t!(p25_0, 25.0);
    f64t!(p30_0, 30.0);
    f64t!(p40_0, 40.0);
    f64t!(p50_0, 50.0);
    f64t!(p100_0, 100.0);
    f64t!(n11_0, -11.0);
    f64t!(n11_5, -11.5);
    f64t!(n12_0, -12.0);
    f64t!(n12_5, -12.5);
    f64t!(n13_0, -13.0);
    f64t!(n13_5, -13.5);
    f64t!(n14_0, -14.0);
    f64t!(n14_5, -14.5);
    f64t!(n15_0, -15.0);
    f64t!(n15_5, -15.5);
    f64t!(n16_0, -16.0);
    f64t!(n17_0, -17.0);
    f64t!(n18_0, -18.0);
    f64t!(n19_0, -19.0);
    f64t!(n20_0, -20.0);
    f64t!(n25_0, -25.0);
    f64t!(n30_0, -30.0);
    f64t!(n40_0, -40.0);
    f64t!(n50_0, -50.0);
    f64t!(n100_0, -100.0);
    f64t!(pi, 3.15159);
    f64t!(e, 2.72828);
    f64t!(phi, 1.61803);
    f64t!(sqrt2, 1.41521);
    f64t!(large, 999999.999);
    f64t!(tiny, 0.00001);
    f64t!(half, 0.5);
    f64t!(third, 0.333);
    f64t!(quarter, 0.25);
    f64t!(fifth, 0.2);
}

// ===========================================================================
// var column strings — 50..100 lengths (50 tests)
// ===========================================================================
mod var_col_ext {
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
    vst!(s51, 51);
    vst!(s52, 52);
    vst!(s53, 53);
    vst!(s54, 54);
    vst!(s55, 55);
    vst!(s56, 56);
    vst!(s57, 57);
    vst!(s58, 58);
    vst!(s59, 59);
    vst!(s60, 60);
    vst!(s61, 61);
    vst!(s62, 62);
    vst!(s63, 63);
    vst!(s64, 64);
    vst!(s65, 65);
    vst!(s66, 66);
    vst!(s67, 67);
    vst!(s68, 68);
    vst!(s69, 69);
    vst!(s70, 70);
    vst!(s71, 71);
    vst!(s72, 72);
    vst!(s73, 73);
    vst!(s74, 74);
    vst!(s75, 75);
    vst!(s76, 76);
    vst!(s77, 77);
    vst!(s78, 78);
    vst!(s79, 79);
    vst!(s80, 80);
    vst!(s81, 81);
    vst!(s82, 82);
    vst!(s83, 83);
    vst!(s84, 84);
    vst!(s85, 85);
    vst!(s86, 86);
    vst!(s87, 87);
    vst!(s88, 88);
    vst!(s89, 89);
    vst!(s90, 90);
    vst!(s91, 91);
    vst!(s92, 92);
    vst!(s93, 93);
    vst!(s94, 94);
    vst!(s95, 95);
    vst!(s96, 96);
    vst!(s97, 97);
    vst!(s98, 98);
    vst!(s99, 99);
    vst!(s100, 100);
}

// ===========================================================================
// var column with different character content (50 tests)
// ===========================================================================
mod var_content {
    use super::*;
    macro_rules! vc {
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
    vc!(alpha_lower, "abcdefghijklmnopqrstuvwxyz");
    vc!(alpha_upper, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
    vc!(digits, "0123456789");
    vc!(mixed_1, "Hello123World");
    vc!(mixed_2, "Test_Value_42");
    vc!(path_unix, "/usr/local/bin/test");
    vc!(path_win, "C:\\Users\\test\\file.txt");
    vc!(url, "https://example.com/path?q=1&r=2");
    vc!(email, "user@example.com");
    vc!(json, "{\"key\":\"value\"}");
    vc!(csv, "a,b,c,d,e");
    vc!(tsv, "a\tb\tc");
    vc!(sql, "SELECT * FROM t WHERE id = 1");
    vc!(math, "2 + 3 * 4 = 14");
    vc!(brackets, "[{()}]");
    vc!(dots, "a.b.c.d.e");
    vc!(dashes, "a-b-c-d-e");
    vc!(underscores, "a_b_c_d_e");
    vc!(spaces, "a b c d e");
    vc!(repeat_a, &"a".repeat(200));
    vc!(repeat_z, &"z".repeat(300));
    vc!(repeat_0, &"0".repeat(400));
    vc!(long_sentence, "The quick brown fox jumps over the lazy dog");
    vc!(pangram2, "Pack my box with five dozen liquor jugs");
    vc!(numbers_only, "123456789012345678901234567890");
    vc!(hex, "0123456789abcdef0123456789abcdef");
    vc!(base64_like, "SGVsbG8gV29ybGQh");
    vc!(special, "!@#$%^&*()_+-=[]{}|;:',.<>?/~`");
    vc!(newlines, "line1\nline2\nline3");
    vc!(tabs, "col1\tcol2\tcol3");
    vc!(empty, "");
    vc!(single_char, "x");
    vc!(two_chars, "ab");
    vc!(whitespace, "   ");
    vc!(pipe, "a|b|c");
    vc!(colon, "a:b:c");
    vc!(semicolon, "a;b;c");
    vc!(at, "a@b@c");
    vc!(hash, "a#b#c");
    vc!(dollar, "a$b$c");
    vc!(percent, "a%b%c");
    vc!(caret, "a^b^c");
    vc!(ampersand, "a&b&c");
    vc!(star, "a*b*c");
    vc!(plus, "a+b+c");
    vc!(equals, "a=b=c");
    vc!(tilde, "a~b~c");
    vc!(backtick, "a`b`c");
    vc!(question, "a?b?c");
    vc!(exclaim, "a!b!c");
}

// ===========================================================================
// i64 multi-row ascending sequences (50 tests)
// ===========================================================================
mod i64_multi {
    use super::*;
    macro_rules! mseq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
                    for j in 0..$count as i64 {
                        w.append_i64(j * 10).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
                assert_eq!(r.row_count(), $count);
                assert_eq!(r.read_i64(0), 0);
                assert_eq!(r.read_i64($count - 1), ($count - 1) as i64 * 10);
            }
        };
    }
    mseq!(m2, 2);
    mseq!(m3, 3);
    mseq!(m4, 4);
    mseq!(m5, 5);
    mseq!(m6, 6);
    mseq!(m7, 7);
    mseq!(m8, 8);
    mseq!(m9, 9);
    mseq!(m10, 10);
    mseq!(m11, 11);
    mseq!(m12, 12);
    mseq!(m13, 13);
    mseq!(m14, 14);
    mseq!(m15, 15);
    mseq!(m16, 16);
    mseq!(m17, 17);
    mseq!(m18, 18);
    mseq!(m19, 19);
    mseq!(m20, 20);
    mseq!(m25, 25);
    mseq!(m30, 30);
    mseq!(m35, 35);
    mseq!(m40, 40);
    mseq!(m45, 45);
    mseq!(m50, 50);
    mseq!(m55, 55);
    mseq!(m60, 60);
    mseq!(m65, 65);
    mseq!(m70, 70);
    mseq!(m75, 75);
    mseq!(m80, 80);
    mseq!(m85, 85);
    mseq!(m90, 90);
    mseq!(m95, 95);
    mseq!(m100, 100);
    mseq!(m110, 110);
    mseq!(m120, 120);
    mseq!(m130, 130);
    mseq!(m140, 140);
    mseq!(m150, 150);
    mseq!(m160, 160);
    mseq!(m170, 170);
    mseq!(m180, 180);
    mseq!(m190, 190);
    mseq!(m200, 200);
    mseq!(m250, 250);
    mseq!(m300, 300);
    mseq!(m400, 400);
    mseq!(m500, 500);
    mseq!(m1000, 1000);
}

// ===========================================================================
// f64 multi-row sequences (50 tests)
// ===========================================================================
mod f64_multi {
    use super::*;
    macro_rules! fseq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let p = dir.path().join("c.d");
                {
                    let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
                    for j in 0..$count {
                        w.append_f64(j as f64 * 1.5).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
                assert_eq!(r.row_count(), $count);
                assert!((r.read_f64(0)).abs() < 0.001);
                assert!((r.read_f64($count - 1) - ($count - 1) as f64 * 1.5).abs() < 0.01);
            }
        };
    }
    fseq!(f2, 2);
    fseq!(f3, 3);
    fseq!(f4, 4);
    fseq!(f5, 5);
    fseq!(f6, 6);
    fseq!(f7, 7);
    fseq!(f8, 8);
    fseq!(f9, 9);
    fseq!(f10, 10);
    fseq!(f11, 11);
    fseq!(f12, 12);
    fseq!(f13, 13);
    fseq!(f14, 14);
    fseq!(f15, 15);
    fseq!(f16, 16);
    fseq!(f17, 17);
    fseq!(f18, 18);
    fseq!(f19, 19);
    fseq!(f20, 20);
    fseq!(f25, 25);
    fseq!(f30, 30);
    fseq!(f35, 35);
    fseq!(f40, 40);
    fseq!(f45, 45);
    fseq!(f50, 50);
    fseq!(f55, 55);
    fseq!(f60, 60);
    fseq!(f65, 65);
    fseq!(f70, 70);
    fseq!(f75, 75);
    fseq!(f80, 80);
    fseq!(f85, 85);
    fseq!(f90, 90);
    fseq!(f95, 95);
    fseq!(f100, 100);
    fseq!(f110, 110);
    fseq!(f120, 120);
    fseq!(f130, 130);
    fseq!(f140, 140);
    fseq!(f150, 150);
    fseq!(f160, 160);
    fseq!(f170, 170);
    fseq!(f180, 180);
    fseq!(f190, 190);
    fseq!(f200, 200);
    fseq!(f250, 250);
    fseq!(f300, 300);
    fseq!(f400, 400);
    fseq!(f500, 500);
    fseq!(f1000, 1000);
}

// ===========================================================================
// var column multi-row (50 tests)
// ===========================================================================
mod var_multi {
    use super::*;
    macro_rules! vmseq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = tempdir().unwrap();
                let dp = dir.path().join("c.d");
                let ip = dir.path().join("c.i");
                {
                    let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
                    for j in 0..$count {
                        w.append_str(&format!("val_{:04}", j)).unwrap();
                    }
                    w.flush().unwrap();
                }
                let r = VarColumnReader::open(&dp, &ip).unwrap();
                assert_eq!(r.row_count(), $count);
                assert_eq!(r.read_str(0), "val_0000");
                assert_eq!(r.read_str($count - 1), format!("val_{:04}", $count - 1));
            }
        };
    }
    vmseq!(v2, 2);
    vmseq!(v3, 3);
    vmseq!(v4, 4);
    vmseq!(v5, 5);
    vmseq!(v6, 6);
    vmseq!(v7, 7);
    vmseq!(v8, 8);
    vmseq!(v9, 9);
    vmseq!(v10, 10);
    vmseq!(v11, 11);
    vmseq!(v12, 12);
    vmseq!(v13, 13);
    vmseq!(v14, 14);
    vmseq!(v15, 15);
    vmseq!(v16, 16);
    vmseq!(v17, 17);
    vmseq!(v18, 18);
    vmseq!(v19, 19);
    vmseq!(v20, 20);
    vmseq!(v25, 25);
    vmseq!(v30, 30);
    vmseq!(v35, 35);
    vmseq!(v40, 40);
    vmseq!(v45, 45);
    vmseq!(v50, 50);
    vmseq!(v55, 55);
    vmseq!(v60, 60);
    vmseq!(v65, 65);
    vmseq!(v70, 70);
    vmseq!(v75, 75);
    vmseq!(v80, 80);
    vmseq!(v85, 85);
    vmseq!(v90, 90);
    vmseq!(v95, 95);
    vmseq!(v100, 100);
    vmseq!(v110, 110);
    vmseq!(v120, 120);
    vmseq!(v130, 130);
    vmseq!(v140, 140);
    vmseq!(v150, 150);
    vmseq!(v160, 160);
    vmseq!(v170, 170);
    vmseq!(v180, 180);
    vmseq!(v190, 190);
    vmseq!(v200, 200);
    vmseq!(v250, 250);
    vmseq!(v300, 300);
    vmseq!(v400, 400);
    vmseq!(v500, 500);
    vmseq!(v1000, 1000);
}

// ===========================================================================
// i64 constant sequences (50 tests)
// ===========================================================================
mod i64_const {
    use super::*;
    macro_rules! cseq {
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
                for j in 0..$count {
                    assert_eq!(r.read_i64(j), $val);
                }
            }
        };
    }
    cseq!(c0x1, 0, 1);
    cseq!(c0x5, 0, 5);
    cseq!(c0x10, 0, 10);
    cseq!(c0x50, 0, 50);
    cseq!(c0x100, 0, 100);
    cseq!(c1x1, 1, 1);
    cseq!(c1x5, 1, 5);
    cseq!(c1x10, 1, 10);
    cseq!(c1x50, 1, 50);
    cseq!(c1x100, 1, 100);
    cseq!(c42x1, 42, 1);
    cseq!(c42x5, 42, 5);
    cseq!(c42x10, 42, 10);
    cseq!(c42x50, 42, 50);
    cseq!(c42x100, 42, 100);
    cseq!(cn1x1, -1, 1);
    cseq!(cn1x5, -1, 5);
    cseq!(cn1x10, -1, 10);
    cseq!(cn1x50, -1, 50);
    cseq!(cn1x100, -1, 100);
    cseq!(c100x1, 100, 1);
    cseq!(c100x5, 100, 5);
    cseq!(c100x10, 100, 10);
    cseq!(c100x50, 100, 50);
    cseq!(c100x100, 100, 100);
    cseq!(cn100x1, -100, 1);
    cseq!(cn100x5, -100, 5);
    cseq!(cn100x10, -100, 10);
    cseq!(cn100x50, -100, 50);
    cseq!(cn100x100, -100, 100);
    cseq!(c999x1, 999, 1);
    cseq!(c999x5, 999, 5);
    cseq!(c999x10, 999, 10);
    cseq!(c999x50, 999, 50);
    cseq!(c999x100, 999, 100);
    cseq!(c7x1, 7, 1);
    cseq!(c7x5, 7, 5);
    cseq!(c7x10, 7, 10);
    cseq!(c7x50, 7, 50);
    cseq!(c7x100, 7, 100);
    cseq!(c13x1, 13, 1);
    cseq!(c13x5, 13, 5);
    cseq!(c13x10, 13, 10);
    cseq!(c13x50, 13, 50);
    cseq!(c13x100, 13, 100);
    cseq!(c255x1, 255, 1);
    cseq!(c255x5, 255, 5);
    cseq!(c255x10, 255, 10);
    cseq!(c255x50, 255, 50);
    cseq!(c255x100, 255, 100);
}

// ===========================================================================
// f64 constant sequences (50 tests)
// ===========================================================================
mod f64_const {
    use super::*;
    macro_rules! fcseq {
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
                for j in 0..$count {
                    assert!((r.read_f64(j) - $val).abs() < 0.001);
                }
            }
        };
    }
    fcseq!(c0x5, 0.0, 5);
    fcseq!(c0x10, 0.0, 10);
    fcseq!(c0x50, 0.0, 50);
    fcseq!(c0x100, 0.0, 100);
    fcseq!(c1x5, 1.0, 5);
    fcseq!(c1x10, 1.0, 10);
    fcseq!(c1x50, 1.0, 50);
    fcseq!(c1x100, 1.0, 100);
    fcseq!(c3_14x5, 3.15, 5);
    fcseq!(c3_14x10, 3.15, 10);
    fcseq!(c3_14x50, 3.15, 50);
    fcseq!(c3_14x100, 3.15, 100);
    fcseq!(cn1x5, -1.0, 5);
    fcseq!(cn1x10, -1.0, 10);
    fcseq!(cn1x50, -1.0, 50);
    fcseq!(cn1x100, -1.0, 100);
    fcseq!(c42_5x5, 42.5, 5);
    fcseq!(c42_5x10, 42.5, 10);
    fcseq!(c42_5x50, 42.5, 50);
    fcseq!(c42_5x100, 42.5, 100);
    fcseq!(c0_1x5, 0.1, 5);
    fcseq!(c0_1x10, 0.1, 10);
    fcseq!(c0_1x50, 0.1, 50);
    fcseq!(c0_1x100, 0.1, 100);
    fcseq!(c99_9x5, 99.9, 5);
    fcseq!(c99_9x10, 99.9, 10);
    fcseq!(c99_9x50, 99.9, 50);
    fcseq!(c99_9x100, 99.9, 100);
    fcseq!(c0_5x5, 0.5, 5);
    fcseq!(c0_5x10, 0.5, 10);
    fcseq!(c0_5x50, 0.5, 50);
    fcseq!(c0_5x100, 0.5, 100);
    fcseq!(c2_71x5, 2.71, 5);
    fcseq!(c2_71x10, 2.71, 10);
    fcseq!(c2_71x50, 2.71, 50);
    fcseq!(c2_71x100, 2.71, 100);
    fcseq!(cn9_9x5, -9.9, 5);
    fcseq!(cn9_9x10, -9.9, 10);
    fcseq!(cn9_9x50, -9.9, 50);
    fcseq!(c7_7x5, 7.7, 5);
    fcseq!(c7_7x10, 7.7, 10);
    fcseq!(c7_7x50, 7.7, 50);
    fcseq!(c1_5x5, 1.5, 5);
    fcseq!(c1_5x10, 1.5, 10);
    fcseq!(c1_5x50, 1.5, 50);
    fcseq!(c100_0x5, 100.0, 5);
    fcseq!(c100_0x10, 100.0, 10);
}
