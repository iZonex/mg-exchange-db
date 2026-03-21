//! 500 abs/round/floor/ceil tests.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn i(v: i64) -> Value { Value::I64(v) }
fn f(v: f64) -> Value { Value::F64(v) }
fn null() -> Value { Value::Null }
fn ev(name: &str, args: &[Value]) -> Value { evaluate_scalar(name, args).unwrap() }
fn close(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"),
        Value::I64(v) => assert!((*v as f64 - expected).abs() < tol, "expected ~{expected}, got {v}"),
        other => panic!("expected ~{expected}, got {other:?}"),
    }
}

// abs for integers -100 to 100
mod abs_int { use super::*;
    macro_rules! abs_i { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { assert_eq!(ev("abs", &[i($v)]), i($e)); } }; }
    abs_i!(n100, -100, 100); abs_i!(n099, -99, 99); abs_i!(n098, -98, 98); abs_i!(n097, -97, 97);
    abs_i!(n096, -96, 96); abs_i!(n095, -95, 95); abs_i!(n094, -94, 94); abs_i!(n093, -93, 93);
    abs_i!(n092, -92, 92); abs_i!(n091, -91, 91); abs_i!(n090, -90, 90); abs_i!(n089, -89, 89);
    abs_i!(n088, -88, 88); abs_i!(n087, -87, 87); abs_i!(n086, -86, 86); abs_i!(n085, -85, 85);
    abs_i!(n084, -84, 84); abs_i!(n083, -83, 83); abs_i!(n082, -82, 82); abs_i!(n081, -81, 81);
    abs_i!(n080, -80, 80); abs_i!(n079, -79, 79); abs_i!(n078, -78, 78); abs_i!(n077, -77, 77);
    abs_i!(n076, -76, 76); abs_i!(n075, -75, 75); abs_i!(n074, -74, 74); abs_i!(n073, -73, 73);
    abs_i!(n072, -72, 72); abs_i!(n071, -71, 71); abs_i!(n070, -70, 70); abs_i!(n069, -69, 69);
    abs_i!(n068, -68, 68); abs_i!(n067, -67, 67); abs_i!(n066, -66, 66); abs_i!(n065, -65, 65);
    abs_i!(n064, -64, 64); abs_i!(n063, -63, 63); abs_i!(n062, -62, 62); abs_i!(n061, -61, 61);
    abs_i!(n060, -60, 60); abs_i!(n059, -59, 59); abs_i!(n058, -58, 58); abs_i!(n057, -57, 57);
    abs_i!(n056, -56, 56); abs_i!(n055, -55, 55); abs_i!(n054, -54, 54); abs_i!(n053, -53, 53);
    abs_i!(n052, -52, 52); abs_i!(n051, -51, 51); abs_i!(n050, -50, 50); abs_i!(n049, -49, 49);
    abs_i!(n048, -48, 48); abs_i!(n047, -47, 47); abs_i!(n046, -46, 46); abs_i!(n045, -45, 45);
    abs_i!(n044, -44, 44); abs_i!(n043, -43, 43); abs_i!(n042, -42, 42); abs_i!(n041, -41, 41);
    abs_i!(n040, -40, 40); abs_i!(n039, -39, 39); abs_i!(n038, -38, 38); abs_i!(n037, -37, 37);
    abs_i!(n036, -36, 36); abs_i!(n035, -35, 35); abs_i!(n034, -34, 34); abs_i!(n033, -33, 33);
    abs_i!(n032, -32, 32); abs_i!(n031, -31, 31); abs_i!(n030, -30, 30); abs_i!(n029, -29, 29);
    abs_i!(n028, -28, 28); abs_i!(n027, -27, 27); abs_i!(n026, -26, 26); abs_i!(n025, -25, 25);
    abs_i!(n024, -24, 24); abs_i!(n023, -23, 23); abs_i!(n022, -22, 22); abs_i!(n021, -21, 21);
    abs_i!(n020, -20, 20); abs_i!(n019, -19, 19); abs_i!(n018, -18, 18); abs_i!(n017, -17, 17);
    abs_i!(n016, -16, 16); abs_i!(n015, -15, 15); abs_i!(n014, -14, 14); abs_i!(n013, -13, 13);
    abs_i!(n012, -12, 12); abs_i!(n011, -11, 11); abs_i!(n010, -10, 10); abs_i!(n009, -9, 9);
    abs_i!(n008, -8, 8); abs_i!(n007, -7, 7); abs_i!(n006, -6, 6); abs_i!(n005, -5, 5);
    abs_i!(n004, -4, 4); abs_i!(n003, -3, 3); abs_i!(n002, -2, 2); abs_i!(n001, -1, 1);
    abs_i!(p000, 0, 0); abs_i!(p001, 1, 1); abs_i!(p002, 2, 2); abs_i!(p003, 3, 3);
    abs_i!(p004, 4, 4); abs_i!(p005, 5, 5); abs_i!(p010, 10, 10); abs_i!(p020, 20, 20);
    abs_i!(p030, 30, 30); abs_i!(p040, 40, 40); abs_i!(p050, 50, 50); abs_i!(p060, 60, 60);
    abs_i!(p070, 70, 70); abs_i!(p080, 80, 80); abs_i!(p090, 90, 90); abs_i!(p100, 100, 100);
    #[test] fn null_in() { assert_eq!(ev("abs", &[null()]), null()); }
}

// abs for floats
mod abs_float { use super::*;
    macro_rules! abs_f { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { assert_eq!(ev("abs", &[f($v)]), f($e)); } }; }
    abs_f!(n10_0, -10.0, 10.0); abs_f!(n9_5, -9.5, 9.5); abs_f!(n9_0, -9.0, 9.0);
    abs_f!(n8_5, -8.5, 8.5); abs_f!(n8_0, -8.0, 8.0); abs_f!(n7_5, -7.5, 7.5);
    abs_f!(n7_0, -7.0, 7.0); abs_f!(n6_5, -6.5, 6.5); abs_f!(n6_0, -6.0, 6.0);
    abs_f!(n5_5, -5.5, 5.5); abs_f!(n5_0, -5.0, 5.0); abs_f!(n4_5, -4.5, 4.5);
    abs_f!(n4_0, -4.0, 4.0); abs_f!(n3_5, -3.5, 3.5); abs_f!(n3_0, -3.0, 3.0);
    abs_f!(n2_5, -2.5, 2.5); abs_f!(n2_0, -2.0, 2.0); abs_f!(n1_5, -1.5, 1.5);
    abs_f!(n1_0, -1.0, 1.0); abs_f!(n0_5, -0.5, 0.5); abs_f!(p0_0, 0.0, 0.0);
    abs_f!(p0_5, 0.5, 0.5); abs_f!(p1_0, 1.0, 1.0); abs_f!(p1_5, 1.5, 1.5);
    abs_f!(p2_0, 2.0, 2.0); abs_f!(p2_5, 2.5, 2.5); abs_f!(p3_0, 3.0, 3.0);
    abs_f!(p3_5, 3.5, 3.5); abs_f!(p4_0, 4.0, 4.0); abs_f!(p4_5, 4.5, 4.5);
    abs_f!(p5_0, 5.0, 5.0); abs_f!(p5_5, 5.5, 5.5); abs_f!(p6_0, 6.0, 6.0);
    abs_f!(p6_5, 6.5, 6.5); abs_f!(p7_0, 7.0, 7.0); abs_f!(p7_5, 7.5, 7.5);
    abs_f!(p8_0, 8.0, 8.0); abs_f!(p8_5, 8.5, 8.5); abs_f!(p9_0, 9.0, 9.0);
    abs_f!(p9_5, 9.5, 9.5); abs_f!(p10_0, 10.0, 10.0);
}

// floor for floats -10.0 to 10.0 step 0.5
mod floor_floats { use super::*;
    macro_rules! fl { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { assert_eq!(ev("floor", &[f($v)]), f($e)); } }; }
    fl!(n10_0, -10.0, -10.0); fl!(n9_5, -9.5, -10.0); fl!(n9_0, -9.0, -9.0); fl!(n8_5, -8.5, -9.0);
    fl!(n8_0, -8.0, -8.0); fl!(n7_5, -7.5, -8.0); fl!(n7_0, -7.0, -7.0); fl!(n6_5, -6.5, -7.0);
    fl!(n6_0, -6.0, -6.0); fl!(n5_5, -5.5, -6.0); fl!(n5_0, -5.0, -5.0); fl!(n4_5, -4.5, -5.0);
    fl!(n4_0, -4.0, -4.0); fl!(n3_5, -3.5, -4.0); fl!(n3_0, -3.0, -3.0); fl!(n2_5, -2.5, -3.0);
    fl!(n2_0, -2.0, -2.0); fl!(n1_5, -1.5, -2.0); fl!(n1_0, -1.0, -1.0); fl!(n0_5, -0.5, -1.0);
    fl!(p0_0, 0.0, 0.0); fl!(p0_5, 0.5, 0.0); fl!(p1_0, 1.0, 1.0); fl!(p1_5, 1.5, 1.0);
    fl!(p2_0, 2.0, 2.0); fl!(p2_5, 2.5, 2.0); fl!(p3_0, 3.0, 3.0); fl!(p3_5, 3.5, 3.0);
    fl!(p4_0, 4.0, 4.0); fl!(p4_5, 4.5, 4.0); fl!(p5_0, 5.0, 5.0); fl!(p5_5, 5.5, 5.0);
    fl!(p6_0, 6.0, 6.0); fl!(p6_5, 6.5, 6.0); fl!(p7_0, 7.0, 7.0); fl!(p7_5, 7.5, 7.0);
    fl!(p8_0, 8.0, 8.0); fl!(p8_5, 8.5, 8.0); fl!(p9_0, 9.0, 9.0); fl!(p9_5, 9.5, 9.0);
    fl!(p10_0, 10.0, 10.0);
    #[test] fn null_in() { assert_eq!(ev("floor", &[null()]), null()); }
}

// ceil for floats -10.0 to 10.0 step 0.5
mod ceil_floats { use super::*;
    macro_rules! cl { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { assert_eq!(ev("ceil", &[f($v)]), f($e)); } }; }
    cl!(n10_0, -10.0, -10.0); cl!(n9_5, -9.5, -9.0); cl!(n9_0, -9.0, -9.0); cl!(n8_5, -8.5, -8.0);
    cl!(n8_0, -8.0, -8.0); cl!(n7_5, -7.5, -7.0); cl!(n7_0, -7.0, -7.0); cl!(n6_5, -6.5, -6.0);
    cl!(n6_0, -6.0, -6.0); cl!(n5_5, -5.5, -5.0); cl!(n5_0, -5.0, -5.0); cl!(n4_5, -4.5, -4.0);
    cl!(n4_0, -4.0, -4.0); cl!(n3_5, -3.5, -3.0); cl!(n3_0, -3.0, -3.0); cl!(n2_5, -2.5, -2.0);
    cl!(n2_0, -2.0, -2.0); cl!(n1_5, -1.5, -1.0); cl!(n1_0, -1.0, -1.0); cl!(n0_5, -0.5, 0.0);
    cl!(p0_0, 0.0, 0.0); cl!(p0_5, 0.5, 1.0); cl!(p1_0, 1.0, 1.0); cl!(p1_5, 1.5, 2.0);
    cl!(p2_0, 2.0, 2.0); cl!(p2_5, 2.5, 3.0); cl!(p3_0, 3.0, 3.0); cl!(p3_5, 3.5, 4.0);
    cl!(p4_0, 4.0, 4.0); cl!(p4_5, 4.5, 5.0); cl!(p5_0, 5.0, 5.0); cl!(p5_5, 5.5, 6.0);
    cl!(p6_0, 6.0, 6.0); cl!(p6_5, 6.5, 7.0); cl!(p7_0, 7.0, 7.0); cl!(p7_5, 7.5, 8.0);
    cl!(p8_0, 8.0, 8.0); cl!(p8_5, 8.5, 9.0); cl!(p9_0, 9.0, 9.0); cl!(p9_5, 9.5, 10.0);
    cl!(p10_0, 10.0, 10.0);
    #[test] fn null_in() { assert_eq!(ev("ceil", &[null()]), null()); }
}

// round for floats
mod round_floats { use super::*;
    macro_rules! rn { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { close(&ev("round", &[f($v)]), $e, 0.01); } }; }
    rn!(n10_0, -10.0, -10.0); rn!(n9_5, -9.5, -10.0); rn!(n9_0, -9.0, -9.0); rn!(n8_5, -8.5, -9.0);
    rn!(n8_0, -8.0, -8.0); rn!(n7_5, -7.5, -8.0); rn!(n7_0, -7.0, -7.0); rn!(n6_5, -6.5, -7.0);
    rn!(n6_0, -6.0, -6.0); rn!(n5_5, -5.5, -6.0); rn!(n5_0, -5.0, -5.0); rn!(n4_5, -4.5, -5.0);
    rn!(n4_0, -4.0, -4.0); rn!(n3_5, -3.5, -4.0); rn!(n3_0, -3.0, -3.0); rn!(n2_5, -2.5, -3.0);
    rn!(n2_0, -2.0, -2.0); rn!(n1_5, -1.5, -2.0); rn!(n1_0, -1.0, -1.0); rn!(n0_5, -0.5, -1.0);
    rn!(p0_0, 0.0, 0.0); rn!(p0_5, 0.5, 1.0); rn!(p1_0, 1.0, 1.0); rn!(p1_5, 1.5, 2.0);
    rn!(p2_0, 2.0, 2.0); rn!(p2_5, 2.5, 3.0); rn!(p3_0, 3.0, 3.0); rn!(p3_5, 3.5, 4.0);
    rn!(p4_0, 4.0, 4.0); rn!(p4_5, 4.5, 5.0); rn!(p5_0, 5.0, 5.0); rn!(p5_5, 5.5, 6.0);
    rn!(p6_0, 6.0, 6.0); rn!(p6_5, 6.5, 7.0); rn!(p7_0, 7.0, 7.0); rn!(p7_5, 7.5, 8.0);
    rn!(p8_0, 8.0, 8.0); rn!(p8_5, 8.5, 9.0); rn!(p9_0, 9.0, 9.0); rn!(p9_5, 9.5, 10.0);
    rn!(p10_0, 10.0, 10.0);
    #[test] fn null_in() { assert_eq!(ev("round", &[null()]), null()); }
}

// floor for integers
mod floor_int { use super::*;
    macro_rules! fli { ($n:ident, $v:expr) => { #[test] fn $n() { assert_eq!(ev("floor", &[i($v)]), f($v as f64)); } }; }
    fli!(n50, -50); fli!(n49, -49); fli!(n48, -48); fli!(n47, -47); fli!(n46, -46);
    fli!(n45, -45); fli!(n44, -44); fli!(n43, -43); fli!(n42, -42); fli!(n41, -41);
    fli!(n40, -40); fli!(n39, -39); fli!(n38, -38); fli!(n37, -37); fli!(n36, -36);
    fli!(n35, -35); fli!(n34, -34); fli!(n33, -33); fli!(n32, -32); fli!(n31, -31);
    fli!(n30, -30); fli!(n29, -29); fli!(n28, -28); fli!(n27, -27); fli!(n26, -26);
    fli!(n25, -25); fli!(n24, -24); fli!(n23, -23); fli!(n22, -22); fli!(n21, -21);
    fli!(n20, -20); fli!(n19, -19); fli!(n18, -18); fli!(n17, -17); fli!(n16, -16);
    fli!(n15, -15); fli!(n14, -14); fli!(n13, -13); fli!(n12, -12); fli!(n11, -11);
    fli!(n10, -10); fli!(n9, -9); fli!(n8, -8); fli!(n7, -7); fli!(n6, -6);
    fli!(n5, -5); fli!(n4, -4); fli!(n3, -3); fli!(n2, -2); fli!(n1, -1);
    fli!(p0, 0); fli!(p1, 1); fli!(p2, 2); fli!(p3, 3); fli!(p4, 4); fli!(p5, 5);
    fli!(p10, 10); fli!(p15, 15); fli!(p20, 20); fli!(p25, 25); fli!(p30, 30);
    fli!(p35, 35); fli!(p40, 40); fli!(p45, 45); fli!(p50, 50);
}

// ceil for integers
mod ceil_int { use super::*;
    macro_rules! cli { ($n:ident, $v:expr) => { #[test] fn $n() { assert_eq!(ev("ceil", &[i($v)]), f($v as f64)); } }; }
    cli!(n50, -50); cli!(n49, -49); cli!(n48, -48); cli!(n47, -47); cli!(n46, -46);
    cli!(n45, -45); cli!(n44, -44); cli!(n43, -43); cli!(n42, -42); cli!(n41, -41);
    cli!(n40, -40); cli!(n39, -39); cli!(n38, -38); cli!(n37, -37); cli!(n36, -36);
    cli!(n35, -35); cli!(n34, -34); cli!(n33, -33); cli!(n32, -32); cli!(n31, -31);
    cli!(n30, -30); cli!(n29, -29); cli!(n28, -28); cli!(n27, -27); cli!(n26, -26);
    cli!(n25, -25); cli!(n24, -24); cli!(n23, -23); cli!(n22, -22); cli!(n21, -21);
    cli!(n20, -20); cli!(n19, -19); cli!(n18, -18); cli!(n17, -17); cli!(n16, -16);
    cli!(n15, -15); cli!(n14, -14); cli!(n13, -13); cli!(n12, -12); cli!(n11, -11);
    cli!(n10, -10); cli!(n9, -9); cli!(n8, -8); cli!(n7, -7); cli!(n6, -6);
    cli!(n5, -5); cli!(n4, -4); cli!(n3, -3); cli!(n2, -2); cli!(n1, -1);
    cli!(p0, 0); cli!(p1, 1); cli!(p2, 2); cli!(p3, 3); cli!(p4, 4); cli!(p5, 5);
    cli!(p10, 10); cli!(p15, 15); cli!(p20, 20); cli!(p25, 25); cli!(p30, 30);
    cli!(p35, 35); cli!(p40, 40); cli!(p45, 45); cli!(p50, 50);
}

// round with decimal places
mod round_dec { use super::*;
    macro_rules! rd { ($n:ident, $v:expr, $d:expr, $e:expr) => { #[test] fn $n() { close(&ev("round", &[f($v), i($d)]), $e, 0.001); } }; }
    rd!(r01, 3.14159, 0, 3.0); rd!(r02, 3.14159, 1, 3.1); rd!(r03, 3.14159, 2, 3.14);
    rd!(r04, 3.14159, 3, 3.142); rd!(r05, 3.14159, 4, 3.1416);
    rd!(r06, 2.71828, 0, 3.0); rd!(r07, 2.71828, 1, 2.7); rd!(r08, 2.71828, 2, 2.72);
    rd!(r09, 2.71828, 3, 2.718); rd!(r10, 2.71828, 4, 2.7183);
    rd!(r11, 1.23456, 0, 1.0); rd!(r12, 1.23456, 1, 1.2); rd!(r13, 1.23456, 2, 1.23);
    rd!(r14, 1.23456, 3, 1.235); rd!(r15, 1.23456, 4, 1.2346);
    rd!(r16, -1.23456, 0, -1.0); rd!(r17, -1.23456, 1, -1.2); rd!(r18, -1.23456, 2, -1.23);
    rd!(r19, 0.5, 0, 1.0); rd!(r20, -0.5, 0, -1.0);
    rd!(r21, 9.99, 1, 10.0); rd!(r22, 9.99, 0, 10.0); rd!(r23, 0.001, 2, 0.0);
    rd!(r24, 0.009, 2, 0.01); rd!(r25, 0.999, 2, 1.0);
    rd!(r26, 100.456, 1, 100.5); rd!(r27, 100.456, 2, 100.46);
    rd!(r28, -100.456, 1, -100.5); rd!(r29, -100.456, 2, -100.46);
    rd!(r30, 0.0, 5, 0.0);
}

// floor for additional float values
mod floor_extra { use super::*;
    macro_rules! fl { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { assert_eq!(ev("floor", &[f($v)]), f($e)); } }; }
    fl!(f01, 0.1, 0.0); fl!(f02, 0.2, 0.0); fl!(f03, 0.3, 0.0); fl!(f04, 0.4, 0.0);
    fl!(f05, 0.6, 0.0); fl!(f06, 0.7, 0.0); fl!(f07, 0.8, 0.0); fl!(f08, 0.9, 0.0);
    fl!(f09, 0.99, 0.0); fl!(f10, 0.01, 0.0); fl!(f11, 1.1, 1.0); fl!(f12, 1.9, 1.0);
    fl!(f13, 2.1, 2.0); fl!(f14, 2.9, 2.0); fl!(f15, 3.1, 3.0); fl!(f16, 3.9, 3.0);
    fl!(f17, 4.1, 4.0); fl!(f18, 4.9, 4.0); fl!(f19, 5.1, 5.0); fl!(f20, 5.9, 5.0);
    fl!(f21, -0.1, -1.0); fl!(f22, -0.9, -1.0); fl!(f23, -1.1, -2.0); fl!(f24, -1.9, -2.0);
    fl!(f25, -2.1, -3.0); fl!(f26, -2.9, -3.0); fl!(f27, -3.1, -4.0); fl!(f28, -3.9, -4.0);
    fl!(f29, -4.1, -5.0); fl!(f30, -4.9, -5.0);
}

// ceil for additional float values
mod ceil_extra { use super::*;
    macro_rules! cl { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { assert_eq!(ev("ceil", &[f($v)]), f($e)); } }; }
    cl!(c01, 0.1, 1.0); cl!(c02, 0.2, 1.0); cl!(c03, 0.3, 1.0); cl!(c04, 0.4, 1.0);
    cl!(c05, 0.6, 1.0); cl!(c06, 0.7, 1.0); cl!(c07, 0.8, 1.0); cl!(c08, 0.9, 1.0);
    cl!(c09, 0.99, 1.0); cl!(c10, 0.01, 1.0); cl!(c11, 1.1, 2.0); cl!(c12, 1.9, 2.0);
    cl!(c13, 2.1, 3.0); cl!(c14, 2.9, 3.0); cl!(c15, 3.1, 4.0); cl!(c16, 3.9, 4.0);
    cl!(c17, 4.1, 5.0); cl!(c18, 4.9, 5.0); cl!(c19, 5.1, 6.0); cl!(c20, 5.9, 6.0);
    cl!(c21, -0.1, 0.0); cl!(c22, -0.9, 0.0); cl!(c23, -1.1, -1.0); cl!(c24, -1.9, -1.0);
    cl!(c25, -2.1, -2.0); cl!(c26, -2.9, -2.0); cl!(c27, -3.1, -3.0); cl!(c28, -3.9, -3.0);
    cl!(c29, -4.1, -4.0); cl!(c30, -4.9, -4.0);
}

// trunc for floats -10.0 to 10.0 step 0.5
mod trunc_floats { use super::*;
    macro_rules! tr { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { close(&ev("trunc", &[f($v)]), $e, 0.01); } }; }
    tr!(n10_0, -10.0, -10.0); tr!(n9_5, -9.5, -9.0); tr!(n9_0, -9.0, -9.0); tr!(n8_5, -8.5, -8.0);
    tr!(n8_0, -8.0, -8.0); tr!(n7_5, -7.5, -7.0); tr!(n7_0, -7.0, -7.0); tr!(n6_5, -6.5, -6.0);
    tr!(n6_0, -6.0, -6.0); tr!(n5_5, -5.5, -5.0); tr!(n5_0, -5.0, -5.0); tr!(n4_5, -4.5, -4.0);
    tr!(n4_0, -4.0, -4.0); tr!(n3_5, -3.5, -3.0); tr!(n3_0, -3.0, -3.0); tr!(n2_5, -2.5, -2.0);
    tr!(n2_0, -2.0, -2.0); tr!(n1_5, -1.5, -1.0); tr!(n1_0, -1.0, -1.0); tr!(n0_5, -0.5, 0.0);
    tr!(p0_0, 0.0, 0.0); tr!(p0_5, 0.5, 0.0); tr!(p1_0, 1.0, 1.0); tr!(p1_5, 1.5, 1.0);
    tr!(p2_0, 2.0, 2.0); tr!(p2_5, 2.5, 2.0); tr!(p3_0, 3.0, 3.0); tr!(p3_5, 3.5, 3.0);
    tr!(p4_0, 4.0, 4.0); tr!(p4_5, 4.5, 4.0); tr!(p5_0, 5.0, 5.0); tr!(p5_5, 5.5, 5.0);
    tr!(p6_0, 6.0, 6.0); tr!(p6_5, 6.5, 6.0); tr!(p7_0, 7.0, 7.0); tr!(p7_5, 7.5, 7.0);
    tr!(p8_0, 8.0, 8.0); tr!(p8_5, 8.5, 8.0); tr!(p9_0, 9.0, 9.0); tr!(p9_5, 9.5, 9.0);
    tr!(p10_0, 10.0, 10.0);
    #[test] fn null_in() { assert_eq!(ev("trunc", &[null()]), null()); }
}

// abs for larger integers
mod abs_large { use super::*;
    macro_rules! al { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { assert_eq!(ev("abs", &[i($v)]), i($e)); } }; }
    al!(n200, -200, 200); al!(n300, -300, 300); al!(n400, -400, 400); al!(n500, -500, 500);
    al!(n600, -600, 600); al!(n700, -700, 700); al!(n800, -800, 800); al!(n900, -900, 900);
    al!(n1000, -1000, 1000); al!(n2000, -2000, 2000); al!(n5000, -5000, 5000);
    al!(n10000, -10000, 10000); al!(n50000, -50000, 50000); al!(n100000, -100000, 100000);
    al!(n1000000, -1000000, 1000000);
    al!(p200, 200, 200); al!(p300, 300, 300); al!(p400, 400, 400); al!(p500, 500, 500);
    al!(p1000, 1000, 1000); al!(p10000, 10000, 10000); al!(p100000, 100000, 100000);
    al!(p1000000, 1000000, 1000000);
}

// round for integers (should remain same)
mod round_int { use super::*;
    macro_rules! ri { ($n:ident, $v:expr) => { #[test] fn $n() { close(&ev("round", &[i($v)]), $v as f64, 0.01); } }; }
    ri!(n50, -50); ri!(n49, -49); ri!(n48, -48); ri!(n47, -47); ri!(n46, -46);
    ri!(n45, -45); ri!(n44, -44); ri!(n43, -43); ri!(n42, -42); ri!(n41, -41);
    ri!(n40, -40); ri!(n39, -39); ri!(n38, -38); ri!(n37, -37); ri!(n36, -36);
    ri!(n35, -35); ri!(n34, -34); ri!(n33, -33); ri!(n32, -32); ri!(n31, -31);
    ri!(n30, -30); ri!(n29, -29); ri!(n28, -28); ri!(n27, -27); ri!(n26, -26);
    ri!(n25, -25); ri!(n24, -24); ri!(n23, -23); ri!(n22, -22); ri!(n21, -21);
    ri!(n20, -20); ri!(n19, -19); ri!(n18, -18); ri!(n17, -17); ri!(n16, -16);
    ri!(n15, -15); ri!(n14, -14); ri!(n13, -13); ri!(n12, -12); ri!(n11, -11);
    ri!(n10, -10); ri!(n9, -9); ri!(n8, -8); ri!(n7, -7); ri!(n6, -6);
    ri!(n5, -5); ri!(n4, -4); ri!(n3, -3); ri!(n2, -2); ri!(n1, -1);
    ri!(p0, 0); ri!(p1, 1); ri!(p2, 2); ri!(p3, 3); ri!(p4, 4); ri!(p5, 5);
    ri!(p10, 10); ri!(p15, 15); ri!(p20, 20); ri!(p25, 25); ri!(p30, 30);
    ri!(p35, 35); ri!(p40, 40); ri!(p45, 45); ri!(p50, 50);
}

// floor/ceil aliases
mod aliases { use super::*;
    #[test] fn floor_double_01() { assert_eq!(ev("floor_double", &[f(3.7)]), f(3.0)); }
    #[test] fn floor_double_02() { assert_eq!(ev("floor_double", &[f(-3.2)]), f(-4.0)); }
    #[test] fn round_down_01() { assert_eq!(ev("round_down", &[f(3.7)]), f(3.0)); }
    #[test] fn round_down_02() { assert_eq!(ev("round_down", &[f(-3.2)]), f(-4.0)); }
    #[test] fn ceiling_01() { assert_eq!(ev("ceiling", &[f(3.1)]), f(4.0)); }
    #[test] fn ceiling_02() { assert_eq!(ev("ceiling", &[f(-3.7)]), f(-3.0)); }
    #[test] fn truncate_01() { close(&ev("truncate", &[f(3.7)]), 3.0, 0.01); }
    #[test] fn truncate_02() { close(&ev("truncate", &[f(-3.7)]), -3.0, 0.01); }
    #[test] fn abs_int_01() { assert_eq!(ev("abs_int", &[i(-5)]), i(5)); }
    #[test] fn abs_long_01() { assert_eq!(ev("abs_long", &[i(-5)]), i(5)); }
    #[test] fn abs_double_01() { assert_eq!(ev("abs_double", &[f(-2.0)]), f(2.0)); }
    #[test] fn abs_float_01() { assert_eq!(ev("abs_float", &[f(-1.5)]), f(1.5)); }
}

// floor for large floats
mod floor_large { use super::*;
    macro_rules! fl { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { assert_eq!(ev("floor", &[f($v)]), f($e)); } }; }
    fl!(f50_1, 50.1, 50.0); fl!(f50_9, 50.9, 50.0); fl!(f100_1, 100.1, 100.0);
    fl!(f100_9, 100.9, 100.0); fl!(f200_5, 200.5, 200.0); fl!(f500_3, 500.3, 500.0);
    fl!(f1000_7, 1000.7, 1000.0); fl!(fn50_1, -50.1, -51.0); fl!(fn100_1, -100.1, -101.0);
    fl!(fn200_5, -200.5, -201.0); fl!(fn500_3, -500.3, -501.0); fl!(fn1000_7, -1000.7, -1001.0);
}

// ceil for large floats
mod ceil_large { use super::*;
    macro_rules! cl { ($n:ident, $v:expr, $e:expr) => { #[test] fn $n() { assert_eq!(ev("ceil", &[f($v)]), f($e)); } }; }
    cl!(c50_1, 50.1, 51.0); cl!(c50_9, 50.9, 51.0); cl!(c100_1, 100.1, 101.0);
    cl!(c100_9, 100.9, 101.0); cl!(c200_5, 200.5, 201.0); cl!(c500_3, 500.3, 501.0);
    cl!(c1000_7, 1000.7, 1001.0); cl!(cn50_1, -50.1, -50.0); cl!(cn100_1, -100.1, -100.0);
    cl!(cn200_5, -200.5, -200.0); cl!(cn500_3, -500.3, -500.0); cl!(cn1000_7, -1000.7, -1000.0);
}
