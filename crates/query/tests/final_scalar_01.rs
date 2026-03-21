//! 500 scalar function tests — remaining untested function×input combinations.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}
fn i(v: i64) -> Value {
    Value::I64(v)
}
fn f(v: f64) -> Value {
    Value::F64(v)
}
fn null() -> Value {
    Value::Null
}
fn ev(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap()
}
#[allow(dead_code)]
fn ev_or(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap_or(Value::Null)
}
fn close(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"),
        Value::I64(v) => assert!(
            (*v as f64 - expected).abs() < tol,
            "expected ~{expected}, got {v}"
        ),
        other => panic!("expected ~{expected}, got {other:?}"),
    }
}

// ===========================================================================
// length — 50 more tests with varied inputs
// ===========================================================================
mod length_f01 {
    use super::*;
    #[test]
    fn la00() {
        assert_eq!(ev("length", &[s("abcdefghijklmnop")]), i(16));
    }
    #[test]
    fn la01() {
        assert_eq!(ev("length", &[s("abcdefghijklmnopq")]), i(17));
    }
    #[test]
    fn la02() {
        assert_eq!(ev("length", &[s("abcdefghijklmnopqr")]), i(18));
    }
    #[test]
    fn la03() {
        assert_eq!(ev("length", &[s("abcdefghijklmnopqrs")]), i(19));
    }
    #[test]
    fn la04() {
        assert_eq!(ev("length", &[s("abcdefghijklmnopqrst")]), i(20));
    }
    #[test]
    fn la05() {
        assert_eq!(ev("len", &[s("abcde")]), i(5));
    }
    #[test]
    fn la06() {
        assert_eq!(ev("len", &[s("")]), i(0));
    }
    #[test]
    fn la07() {
        assert_eq!(ev("len", &[s("x")]), i(1));
    }
    #[test]
    fn la08() {
        assert_eq!(ev("len", &[null()]), null());
    }
    #[test]
    fn la09() {
        assert_eq!(ev("char_length", &[s("abcd")]), i(4));
    }
    #[test]
    fn la10() {
        assert_eq!(ev("char_length", &[s("")]), i(0));
    }
    #[test]
    fn la11() {
        assert_eq!(ev("char_length", &[null()]), null());
    }
    #[test]
    fn la12() {
        assert_eq!(ev("string_length", &[s("abc")]), i(3));
    }
    #[test]
    fn la13() {
        assert_eq!(ev("string_length", &[null()]), null());
    }
    #[test]
    fn la14() {
        assert_eq!(ev("length", &[s(&"m".repeat(30))]), i(30));
    }
    #[test]
    fn la15() {
        assert_eq!(ev("length", &[s(&"n".repeat(40))]), i(40));
    }
    #[test]
    fn la16() {
        assert_eq!(ev("length", &[s(&"o".repeat(60))]), i(60));
    }
    #[test]
    fn la17() {
        assert_eq!(ev("length", &[s(&"p".repeat(70))]), i(70));
    }
    #[test]
    fn la18() {
        assert_eq!(ev("length", &[s(&"q".repeat(80))]), i(80));
    }
    #[test]
    fn la19() {
        assert_eq!(ev("length", &[s(&"r".repeat(90))]), i(90));
    }
    #[test]
    fn la20() {
        assert_eq!(ev("length", &[s(&"s".repeat(150))]), i(150));
    }
    #[test]
    fn la21() {
        assert_eq!(ev("length", &[s(&"t".repeat(250))]), i(250));
    }
    #[test]
    fn la22() {
        assert_eq!(ev("length", &[s(&"u".repeat(300))]), i(300));
    }
    #[test]
    fn la23() {
        assert_eq!(ev("length", &[s(&"v".repeat(400))]), i(400));
    }
    #[test]
    fn la24() {
        assert_eq!(ev("length", &[s(&"w".repeat(600))]), i(600));
    }
    #[test]
    fn la25() {
        assert_eq!(ev("length", &[s(&"x".repeat(700))]), i(700));
    }
    #[test]
    fn la26() {
        assert_eq!(ev("length", &[s(&"y".repeat(800))]), i(800));
    }
    #[test]
    fn la27() {
        assert_eq!(ev("length", &[s(&"z".repeat(900))]), i(900));
    }
    #[test]
    fn la28() {
        assert_eq!(ev("length", &[i(999)]), i(3));
    }
    #[test]
    fn la29() {
        assert_eq!(ev("length", &[i(10000)]), i(5));
    }
    #[test]
    fn la30() {
        assert_eq!(ev("length", &[i(-100)]), i(4));
    }
    #[test]
    fn la31() {
        assert_eq!(ev("length", &[i(-1000)]), i(5));
    }
    #[test]
    fn la32() {
        assert_eq!(ev("length", &[i(1)]), i(1));
    }
    #[test]
    fn la33() {
        assert_eq!(ev("length", &[i(9)]), i(1));
    }
    #[test]
    fn la34() {
        assert_eq!(ev("length", &[i(10)]), i(2));
    }
    #[test]
    fn la35() {
        assert_eq!(ev("length", &[i(99)]), i(2));
    }
    #[test]
    fn la36() {
        assert_eq!(ev("length", &[i(100)]), i(3));
    }
    #[test]
    fn la37() {
        assert_eq!(ev("byte_length", &[s("abcdef")]), i(6));
    }
    #[test]
    fn la38() {
        assert_eq!(ev("byte_length", &[s("abcdefghij")]), i(10));
    }
    #[test]
    fn la39() {
        assert_eq!(ev("byte_length", &[s("a")]), i(1));
    }
    #[test]
    fn la40() {
        assert_eq!(ev("octet_length", &[s("hello")]), i(5));
    }
    #[test]
    fn la41() {
        assert_eq!(ev("octet_length", &[s("")]), i(0));
    }
    #[test]
    fn la42() {
        assert_eq!(ev("octet_length", &[null()]), null());
    }
    #[test]
    fn la43() {
        assert_eq!(ev("bit_length", &[s("abc")]), i(24));
    }
    #[test]
    fn la44() {
        assert_eq!(ev("bit_length", &[s("abcd")]), i(32));
    }
    #[test]
    fn la45() {
        assert_eq!(ev("bit_length", &[s("abcde")]), i(40));
    }
    #[test]
    fn la46() {
        assert_eq!(ev("word_count", &[s("a b c d e")]), i(5));
    }
    #[test]
    fn la47() {
        assert_eq!(ev("word_count", &[s("hello")]), i(1));
    }
    #[test]
    fn la48() {
        assert_eq!(ev("word_count", &[s("")]), i(0));
    }
    #[test]
    fn la49() {
        assert_eq!(ev("word_count", &[null()]), null());
    }
}

// ===========================================================================
// upper — 30 more tests
// ===========================================================================
mod upper_f01 {
    use super::*;
    #[test]
    fn u00() {
        assert_eq!(ev("upper", &[s("database")]), s("DATABASE"));
    }
    #[test]
    fn u01() {
        assert_eq!(ev("upper", &[s("query")]), s("QUERY"));
    }
    #[test]
    fn u02() {
        assert_eq!(ev("upper", &[s("exchange")]), s("EXCHANGE"));
    }
    #[test]
    fn u03() {
        assert_eq!(ev("upper", &[s("table")]), s("TABLE"));
    }
    #[test]
    fn u04() {
        assert_eq!(ev("upper", &[s("column")]), s("COLUMN"));
    }
    #[test]
    fn u05() {
        assert_eq!(ev("upper", &[s("index")]), s("INDEX"));
    }
    #[test]
    fn u06() {
        assert_eq!(ev("upper", &[s("select")]), s("SELECT"));
    }
    #[test]
    fn u07() {
        assert_eq!(ev("upper", &[s("insert")]), s("INSERT"));
    }
    #[test]
    fn u08() {
        assert_eq!(ev("upper", &[s("update")]), s("UPDATE"));
    }
    #[test]
    fn u09() {
        assert_eq!(ev("upper", &[s("delete")]), s("DELETE"));
    }
    #[test]
    fn u10() {
        assert_eq!(ev("upper", &[s("where")]), s("WHERE"));
    }
    #[test]
    fn u11() {
        assert_eq!(ev("upper", &[s("from")]), s("FROM"));
    }
    #[test]
    fn u12() {
        assert_eq!(ev("upper", &[s("order")]), s("ORDER"));
    }
    #[test]
    fn u13() {
        assert_eq!(ev("upper", &[s("group")]), s("GROUP"));
    }
    #[test]
    fn u14() {
        assert_eq!(ev("upper", &[s("having")]), s("HAVING"));
    }
    #[test]
    fn u15() {
        assert_eq!(ev("upper", &[s("limit")]), s("LIMIT"));
    }
    #[test]
    fn u16() {
        assert_eq!(ev("upper", &[s("offset")]), s("OFFSET"));
    }
    #[test]
    fn u17() {
        assert_eq!(ev("upper", &[s("join")]), s("JOIN"));
    }
    #[test]
    fn u18() {
        assert_eq!(ev("upper", &[s("inner")]), s("INNER"));
    }
    #[test]
    fn u19() {
        assert_eq!(ev("upper", &[s("outer")]), s("OUTER"));
    }
    #[test]
    fn u20() {
        assert_eq!(ev("upper", &[s("left")]), s("LEFT"));
    }
    #[test]
    fn u21() {
        assert_eq!(ev("upper", &[s("right")]), s("RIGHT"));
    }
    #[test]
    fn u22() {
        assert_eq!(ev("upper", &[s("cross")]), s("CROSS"));
    }
    #[test]
    fn u23() {
        assert_eq!(ev("upper", &[s("union")]), s("UNION"));
    }
    #[test]
    fn u24() {
        assert_eq!(ev("upper", &[s("except")]), s("EXCEPT"));
    }
    #[test]
    fn u25() {
        assert_eq!(ev("upper", &[s("intersect")]), s("INTERSECT"));
    }
    #[test]
    fn u26() {
        assert_eq!(ev("upper", &[s("create")]), s("CREATE"));
    }
    #[test]
    fn u27() {
        assert_eq!(ev("upper", &[s("drop")]), s("DROP"));
    }
    #[test]
    fn u28() {
        assert_eq!(ev("upper", &[s("alter")]), s("ALTER"));
    }
    #[test]
    fn u29() {
        assert_eq!(ev("upper", &[s("truncate")]), s("TRUNCATE"));
    }
}

// ===========================================================================
// lower — 30 more tests
// ===========================================================================
mod lower_f01 {
    use super::*;
    #[test]
    fn l00() {
        assert_eq!(ev("lower", &[s("DATABASE")]), s("database"));
    }
    #[test]
    fn l01() {
        assert_eq!(ev("lower", &[s("QUERY")]), s("query"));
    }
    #[test]
    fn l02() {
        assert_eq!(ev("lower", &[s("EXCHANGE")]), s("exchange"));
    }
    #[test]
    fn l03() {
        assert_eq!(ev("lower", &[s("TABLE")]), s("table"));
    }
    #[test]
    fn l04() {
        assert_eq!(ev("lower", &[s("COLUMN")]), s("column"));
    }
    #[test]
    fn l05() {
        assert_eq!(ev("lower", &[s("INDEX")]), s("index"));
    }
    #[test]
    fn l06() {
        assert_eq!(ev("lower", &[s("SELECT")]), s("select"));
    }
    #[test]
    fn l07() {
        assert_eq!(ev("lower", &[s("INSERT")]), s("insert"));
    }
    #[test]
    fn l08() {
        assert_eq!(ev("lower", &[s("UPDATE")]), s("update"));
    }
    #[test]
    fn l09() {
        assert_eq!(ev("lower", &[s("DELETE")]), s("delete"));
    }
    #[test]
    fn l10() {
        assert_eq!(ev("lower", &[s("WHERE")]), s("where"));
    }
    #[test]
    fn l11() {
        assert_eq!(ev("lower", &[s("FROM")]), s("from"));
    }
    #[test]
    fn l12() {
        assert_eq!(ev("lower", &[s("ORDER")]), s("order"));
    }
    #[test]
    fn l13() {
        assert_eq!(ev("lower", &[s("GROUP")]), s("group"));
    }
    #[test]
    fn l14() {
        assert_eq!(ev("lower", &[s("HAVING")]), s("having"));
    }
    #[test]
    fn l15() {
        assert_eq!(ev("lower", &[s("LIMIT")]), s("limit"));
    }
    #[test]
    fn l16() {
        assert_eq!(ev("lower", &[s("OFFSET")]), s("offset"));
    }
    #[test]
    fn l17() {
        assert_eq!(ev("lower", &[s("JOIN")]), s("join"));
    }
    #[test]
    fn l18() {
        assert_eq!(ev("lower", &[s("INNER")]), s("inner"));
    }
    #[test]
    fn l19() {
        assert_eq!(ev("lower", &[s("OUTER")]), s("outer"));
    }
    #[test]
    fn l20() {
        assert_eq!(ev("lower", &[s("LEFT")]), s("left"));
    }
    #[test]
    fn l21() {
        assert_eq!(ev("lower", &[s("RIGHT")]), s("right"));
    }
    #[test]
    fn l22() {
        assert_eq!(ev("lower", &[s("CROSS")]), s("cross"));
    }
    #[test]
    fn l23() {
        assert_eq!(ev("lower", &[s("UNION")]), s("union"));
    }
    #[test]
    fn l24() {
        assert_eq!(ev("lower", &[s("EXCEPT")]), s("except"));
    }
    #[test]
    fn l25() {
        assert_eq!(ev("lower", &[s("INTERSECT")]), s("intersect"));
    }
    #[test]
    fn l26() {
        assert_eq!(ev("lower", &[s("CREATE")]), s("create"));
    }
    #[test]
    fn l27() {
        assert_eq!(ev("lower", &[s("DROP")]), s("drop"));
    }
    #[test]
    fn l28() {
        assert_eq!(ev("lower", &[s("ALTER")]), s("alter"));
    }
    #[test]
    fn l29() {
        assert_eq!(ev("lower", &[s("TRUNCATE")]), s("truncate"));
    }
}

// ===========================================================================
// abs — 30 more tests
// ===========================================================================
mod abs_f01 {
    use super::*;
    #[test]
    fn a00() {
        assert_eq!(ev("abs", &[i(-200)]), i(200));
    }
    #[test]
    fn a01() {
        assert_eq!(ev("abs", &[i(-300)]), i(300));
    }
    #[test]
    fn a02() {
        assert_eq!(ev("abs", &[i(-400)]), i(400));
    }
    #[test]
    fn a03() {
        assert_eq!(ev("abs", &[i(-500)]), i(500));
    }
    #[test]
    fn a04() {
        assert_eq!(ev("abs", &[i(-600)]), i(600));
    }
    #[test]
    fn a05() {
        assert_eq!(ev("abs", &[i(-700)]), i(700));
    }
    #[test]
    fn a06() {
        assert_eq!(ev("abs", &[i(-800)]), i(800));
    }
    #[test]
    fn a07() {
        assert_eq!(ev("abs", &[i(-900)]), i(900));
    }
    #[test]
    fn a08() {
        assert_eq!(ev("abs", &[i(-1500)]), i(1500));
    }
    #[test]
    fn a09() {
        assert_eq!(ev("abs", &[i(-2000)]), i(2000));
    }
    #[test]
    fn a10() {
        assert_eq!(ev("abs", &[f(-10.5)]), f(10.5));
    }
    #[test]
    fn a11() {
        assert_eq!(ev("abs", &[f(-20.5)]), f(20.5));
    }
    #[test]
    fn a12() {
        assert_eq!(ev("abs", &[f(-30.5)]), f(30.5));
    }
    #[test]
    fn a13() {
        assert_eq!(ev("abs", &[f(-40.5)]), f(40.5));
    }
    #[test]
    fn a14() {
        assert_eq!(ev("abs", &[f(-50.5)]), f(50.5));
    }
    #[test]
    fn a15() {
        assert_eq!(ev("abs", &[f(-0.001)]), f(0.001));
    }
    #[test]
    fn a16() {
        assert_eq!(ev("abs", &[f(-0.01)]), f(0.01));
    }
    #[test]
    fn a17() {
        assert_eq!(ev("abs", &[f(-0.1)]), f(0.1));
    }
    #[test]
    fn a18() {
        assert_eq!(ev("abs", &[f(-1.1)]), f(1.1));
    }
    #[test]
    fn a19() {
        assert_eq!(ev("abs", &[f(-2.2)]), f(2.2));
    }
    #[test]
    fn a20() {
        assert_eq!(ev("abs", &[f(-3.3)]), f(3.3));
    }
    #[test]
    fn a21() {
        assert_eq!(ev("abs", &[f(-4.4)]), f(4.4));
    }
    #[test]
    fn a22() {
        assert_eq!(ev("abs", &[f(-5.5)]), f(5.5));
    }
    #[test]
    fn a23() {
        assert_eq!(ev("abs", &[f(-6.6)]), f(6.6));
    }
    #[test]
    fn a24() {
        assert_eq!(ev("abs", &[f(-7.7)]), f(7.7));
    }
    #[test]
    fn a25() {
        assert_eq!(ev("abs", &[f(-8.8)]), f(8.8));
    }
    #[test]
    fn a26() {
        assert_eq!(ev("abs", &[f(-9.9)]), f(9.9));
    }
    #[test]
    fn a27() {
        assert_eq!(ev("abs", &[f(-11.11)]), f(11.11));
    }
    #[test]
    fn a28() {
        assert_eq!(ev("abs", &[f(-22.22)]), f(22.22));
    }
    #[test]
    fn a29() {
        assert_eq!(ev("abs", &[f(-99.99)]), f(99.99));
    }
}

// ===========================================================================
// round — 30 more tests
// ===========================================================================
mod round_f01 {
    use super::*;
    #[test]
    fn r00() {
        close(&ev("round", &[f(1.4)]), 1.0, 0.01);
    }
    #[test]
    fn r01() {
        close(&ev("round", &[f(1.5)]), 2.0, 0.01);
    }
    #[test]
    fn r02() {
        close(&ev("round", &[f(1.6)]), 2.0, 0.01);
    }
    #[test]
    fn r03() {
        close(&ev("round", &[f(2.4)]), 2.0, 0.01);
    }
    #[test]
    fn r04() {
        close(&ev("round", &[f(2.5)]), 3.0, 0.01);
    }
    #[test]
    fn r05() {
        close(&ev("round", &[f(2.6)]), 3.0, 0.01);
    }
    #[test]
    fn r06() {
        close(&ev("round", &[f(0.0)]), 0.0, 0.01);
    }
    #[test]
    fn r07() {
        close(&ev("round", &[f(-1.4)]), -1.0, 0.01);
    }
    #[test]
    fn r08() {
        close(&ev("round", &[f(-1.6)]), -2.0, 0.01);
    }
    #[test]
    fn r09() {
        close(&ev("round", &[f(3.15)]), 3.0, 0.01);
    }
    #[test]
    fn r10() {
        close(&ev("round", &[f(3.74)]), 4.0, 0.01);
    }
    #[test]
    fn r11() {
        assert_eq!(ev("round", &[null()]), null());
    }
    #[test]
    fn r12() {
        close(&ev("round", &[f(10.1)]), 10.0, 0.01);
    }
    #[test]
    fn r13() {
        close(&ev("round", &[f(10.9)]), 11.0, 0.01);
    }
    #[test]
    fn r14() {
        close(&ev("round", &[f(99.4)]), 99.0, 0.01);
    }
    #[test]
    fn r15() {
        close(&ev("round", &[f(99.5)]), 100.0, 0.01);
    }
    #[test]
    fn r16() {
        close(&ev("round", &[f(100.1)]), 100.0, 0.01);
    }
    #[test]
    fn r17() {
        close(&ev("round", &[f(100.9)]), 101.0, 0.01);
    }
    #[test]
    fn r18() {
        close(&ev("round", &[f(-0.1)]), 0.0, 0.01);
    }
    #[test]
    fn r19() {
        close(&ev("round", &[f(-0.9)]), -1.0, 0.01);
    }
    #[test]
    fn r20() {
        close(&ev("round", &[i(5)]), 5.0, 0.01);
    }
    #[test]
    fn r21() {
        close(&ev("round", &[i(0)]), 0.0, 0.01);
    }
    #[test]
    fn r22() {
        close(&ev("round", &[i(-5)]), -5.0, 0.01);
    }
    #[test]
    fn r23() {
        close(&ev("round", &[f(0.49)]), 0.0, 0.01);
    }
    #[test]
    fn r24() {
        close(&ev("round", &[f(0.51)]), 1.0, 0.01);
    }
    #[test]
    fn r25() {
        close(&ev("round", &[f(4.5)]), 5.0, 0.01);
    }
    #[test]
    fn r26() {
        close(&ev("round", &[f(5.5)]), 6.0, 0.01);
    }
    #[test]
    fn r27() {
        close(&ev("round", &[f(6.5)]), 7.0, 0.01);
    }
    #[test]
    fn r28() {
        close(&ev("round", &[f(7.5)]), 8.0, 0.01);
    }
    #[test]
    fn r29() {
        close(&ev("round", &[f(8.5)]), 9.0, 0.01);
    }
}

// ===========================================================================
// floor — 30 more tests
// ===========================================================================
mod floor_f01 {
    use super::*;
    #[test]
    fn f00() {
        close(&ev("floor", &[f(1.1)]), 1.0, 0.01);
    }
    #[test]
    fn f01() {
        close(&ev("floor", &[f(1.9)]), 1.0, 0.01);
    }
    #[test]
    fn f02() {
        close(&ev("floor", &[f(2.0)]), 2.0, 0.01);
    }
    #[test]
    fn f03() {
        close(&ev("floor", &[f(2.1)]), 2.0, 0.01);
    }
    #[test]
    fn f04() {
        close(&ev("floor", &[f(2.9)]), 2.0, 0.01);
    }
    #[test]
    fn f05() {
        close(&ev("floor", &[f(0.0)]), 0.0, 0.01);
    }
    #[test]
    fn f06() {
        close(&ev("floor", &[f(-0.1)]), -1.0, 0.01);
    }
    #[test]
    fn f07() {
        close(&ev("floor", &[f(-0.9)]), -1.0, 0.01);
    }
    #[test]
    fn f08() {
        close(&ev("floor", &[f(-1.0)]), -1.0, 0.01);
    }
    #[test]
    fn f09() {
        close(&ev("floor", &[f(-1.1)]), -2.0, 0.01);
    }
    #[test]
    fn f10() {
        close(&ev("floor", &[f(3.15)]), 3.0, 0.01);
    }
    #[test]
    fn f11() {
        close(&ev("floor", &[f(3.99)]), 3.0, 0.01);
    }
    #[test]
    fn f12() {
        assert_eq!(ev("floor", &[null()]), null());
    }
    #[test]
    fn f13() {
        close(&ev("floor", &[i(5)]), 5.0, 0.01);
    }
    #[test]
    fn f14() {
        close(&ev("floor", &[i(0)]), 0.0, 0.01);
    }
    #[test]
    fn f15() {
        close(&ev("floor", &[i(-5)]), -5.0, 0.01);
    }
    #[test]
    fn f16() {
        close(&ev("floor", &[f(10.0)]), 10.0, 0.01);
    }
    #[test]
    fn f17() {
        close(&ev("floor", &[f(10.5)]), 10.0, 0.01);
    }
    #[test]
    fn f18() {
        close(&ev("floor", &[f(10.999)]), 10.0, 0.01);
    }
    #[test]
    fn f19() {
        close(&ev("floor", &[f(99.99)]), 99.0, 0.01);
    }
    #[test]
    fn f20() {
        close(&ev("floor", &[f(100.0)]), 100.0, 0.01);
    }
    #[test]
    fn f21() {
        close(&ev("floor", &[f(-2.5)]), -3.0, 0.01);
    }
    #[test]
    fn f22() {
        close(&ev("floor", &[f(-3.1)]), -4.0, 0.01);
    }
    #[test]
    fn f23() {
        close(&ev("floor", &[f(-10.5)]), -11.0, 0.01);
    }
    #[test]
    fn f24() {
        close(&ev("floor", &[f(0.001)]), 0.0, 0.01);
    }
    #[test]
    fn f25() {
        close(&ev("floor", &[f(0.999)]), 0.0, 0.01);
    }
    #[test]
    fn f26() {
        close(&ev("floor", &[f(1.0)]), 1.0, 0.01);
    }
    #[test]
    fn f27() {
        close(&ev("floor", &[f(50.5)]), 50.0, 0.01);
    }
    #[test]
    fn f28() {
        close(&ev("floor", &[f(-50.5)]), -51.0, 0.01);
    }
    #[test]
    fn f29() {
        close(&ev("floor", &[f(1000.1)]), 1000.0, 0.01);
    }
}

// ===========================================================================
// ceil — 30 more tests
// ===========================================================================
mod ceil_f01 {
    use super::*;
    #[test]
    fn c00() {
        close(&ev("ceil", &[f(1.1)]), 2.0, 0.01);
    }
    #[test]
    fn c01() {
        close(&ev("ceil", &[f(1.9)]), 2.0, 0.01);
    }
    #[test]
    fn c02() {
        close(&ev("ceil", &[f(2.0)]), 2.0, 0.01);
    }
    #[test]
    fn c03() {
        close(&ev("ceil", &[f(2.1)]), 3.0, 0.01);
    }
    #[test]
    fn c04() {
        close(&ev("ceil", &[f(0.0)]), 0.0, 0.01);
    }
    #[test]
    fn c05() {
        close(&ev("ceil", &[f(-0.1)]), 0.0, 0.01);
    }
    #[test]
    fn c06() {
        close(&ev("ceil", &[f(-0.9)]), 0.0, 0.01);
    }
    #[test]
    fn c07() {
        close(&ev("ceil", &[f(-1.0)]), -1.0, 0.01);
    }
    #[test]
    fn c08() {
        close(&ev("ceil", &[f(-1.1)]), -1.0, 0.01);
    }
    #[test]
    fn c09() {
        close(&ev("ceil", &[f(-1.9)]), -1.0, 0.01);
    }
    #[test]
    fn c10() {
        close(&ev("ceil", &[f(3.15)]), 4.0, 0.01);
    }
    #[test]
    fn c11() {
        close(&ev("ceil", &[f(3.01)]), 4.0, 0.01);
    }
    #[test]
    fn c12() {
        assert_eq!(ev("ceil", &[null()]), null());
    }
    #[test]
    fn c13() {
        close(&ev("ceil", &[i(5)]), 5.0, 0.01);
    }
    #[test]
    fn c14() {
        close(&ev("ceil", &[i(0)]), 0.0, 0.01);
    }
    #[test]
    fn c15() {
        close(&ev("ceil", &[i(-5)]), -5.0, 0.01);
    }
    #[test]
    fn c16() {
        close(&ev("ceil", &[f(10.0)]), 10.0, 0.01);
    }
    #[test]
    fn c17() {
        close(&ev("ceil", &[f(10.001)]), 11.0, 0.01);
    }
    #[test]
    fn c18() {
        close(&ev("ceil", &[f(99.99)]), 100.0, 0.01);
    }
    #[test]
    fn c19() {
        close(&ev("ceil", &[f(100.0)]), 100.0, 0.01);
    }
    #[test]
    fn c20() {
        close(&ev("ceil", &[f(100.01)]), 101.0, 0.01);
    }
    #[test]
    fn c21() {
        close(&ev("ceil", &[f(-2.5)]), -2.0, 0.01);
    }
    #[test]
    fn c22() {
        close(&ev("ceil", &[f(-3.1)]), -3.0, 0.01);
    }
    #[test]
    fn c23() {
        close(&ev("ceil", &[f(-10.5)]), -10.0, 0.01);
    }
    #[test]
    fn c24() {
        close(&ev("ceil", &[f(0.001)]), 1.0, 0.01);
    }
    #[test]
    fn c25() {
        close(&ev("ceil", &[f(0.999)]), 1.0, 0.01);
    }
    #[test]
    fn c26() {
        close(&ev("ceil", &[f(50.5)]), 51.0, 0.01);
    }
    #[test]
    fn c27() {
        close(&ev("ceil", &[f(-50.5)]), -50.0, 0.01);
    }
    #[test]
    fn c28() {
        close(&ev("ceiling", &[f(1.1)]), 2.0, 0.01);
    }
    #[test]
    fn c29() {
        close(&ev("ceiling", &[f(-1.1)]), -1.0, 0.01);
    }
}

// ===========================================================================
// sqrt — 20 more tests
// ===========================================================================
mod sqrt_f01 {
    use super::*;
    #[test]
    fn s00() {
        close(&ev("sqrt", &[f(4.0)]), 2.0, 0.01);
    }
    #[test]
    fn s01() {
        close(&ev("sqrt", &[f(9.0)]), 3.0, 0.01);
    }
    #[test]
    fn s02() {
        close(&ev("sqrt", &[f(16.0)]), 4.0, 0.01);
    }
    #[test]
    fn s03() {
        close(&ev("sqrt", &[f(25.0)]), 5.0, 0.01);
    }
    #[test]
    fn s04() {
        close(&ev("sqrt", &[f(36.0)]), 6.0, 0.01);
    }
    #[test]
    fn s05() {
        close(&ev("sqrt", &[f(49.0)]), 7.0, 0.01);
    }
    #[test]
    fn s06() {
        close(&ev("sqrt", &[f(64.0)]), 8.0, 0.01);
    }
    #[test]
    fn s07() {
        close(&ev("sqrt", &[f(81.0)]), 9.0, 0.01);
    }
    #[test]
    fn s08() {
        close(&ev("sqrt", &[f(100.0)]), 10.0, 0.01);
    }
    #[test]
    fn s09() {
        close(&ev("sqrt", &[f(1.0)]), 1.0, 0.01);
    }
    #[test]
    fn s10() {
        close(&ev("sqrt", &[f(0.0)]), 0.0, 0.01);
    }
    #[test]
    fn s11() {
        close(&ev("sqrt", &[i(4)]), 2.0, 0.01);
    }
    #[test]
    fn s12() {
        close(&ev("sqrt", &[i(9)]), 3.0, 0.01);
    }
    #[test]
    fn s13() {
        close(&ev("sqrt", &[i(16)]), 4.0, 0.01);
    }
    #[test]
    fn s14() {
        close(&ev("sqrt", &[i(25)]), 5.0, 0.01);
    }
    #[test]
    fn s15() {
        close(&ev("sqrt", &[i(100)]), 10.0, 0.01);
    }
    #[test]
    fn s16() {
        assert_eq!(ev("sqrt", &[null()]), null());
    }
    #[test]
    fn s17() {
        close(&ev("sqrt", &[f(2.0)]), 1.414, 0.01);
    }
    #[test]
    fn s18() {
        close(&ev("sqrt", &[f(0.25)]), 0.5, 0.01);
    }
    #[test]
    fn s19() {
        close(&ev("sqrt", &[f(144.0)]), 12.0, 0.01);
    }
}

// ===========================================================================
// sign — 20 more tests
// ===========================================================================
mod sign_f01 {
    use super::*;
    #[test]
    fn s00() {
        close(&ev("sign", &[i(5)]), 1.0, 0.01);
    }
    #[test]
    fn s01() {
        close(&ev("sign", &[i(-5)]), -1.0, 0.01);
    }
    #[test]
    fn s02() {
        close(&ev("sign", &[i(0)]), 0.0, 0.01);
    }
    #[test]
    fn s03() {
        close(&ev("sign", &[f(3.15)]), 1.0, 0.01);
    }
    #[test]
    fn s04() {
        close(&ev("sign", &[f(-3.15)]), -1.0, 0.01);
    }
    #[test]
    fn s05() {
        close(&ev("sign", &[f(0.0)]), 0.0, 0.01);
    }
    #[test]
    fn s06() {
        assert_eq!(ev("sign", &[null()]), null());
    }
    #[test]
    fn s07() {
        close(&ev("sign", &[i(1)]), 1.0, 0.01);
    }
    #[test]
    fn s08() {
        close(&ev("sign", &[i(-1)]), -1.0, 0.01);
    }
    #[test]
    fn s09() {
        close(&ev("sign", &[i(100)]), 1.0, 0.01);
    }
    #[test]
    fn s10() {
        close(&ev("sign", &[i(-100)]), -1.0, 0.01);
    }
    #[test]
    fn s11() {
        close(&ev("sign", &[i(1000)]), 1.0, 0.01);
    }
    #[test]
    fn s12() {
        close(&ev("sign", &[i(-1000)]), -1.0, 0.01);
    }
    #[test]
    fn s13() {
        close(&ev("sign", &[f(0.001)]), 1.0, 0.01);
    }
    #[test]
    fn s14() {
        close(&ev("sign", &[f(-0.001)]), -1.0, 0.01);
    }
    #[test]
    fn s15() {
        close(&ev("sign", &[f(999.0)]), 1.0, 0.01);
    }
    #[test]
    fn s16() {
        close(&ev("sign", &[f(-999.0)]), -1.0, 0.01);
    }
    #[test]
    fn s17() {
        close(&ev("sign", &[i(42)]), 1.0, 0.01);
    }
    #[test]
    fn s18() {
        close(&ev("sign", &[i(-42)]), -1.0, 0.01);
    }
    #[test]
    fn s19() {
        close(&ev("sign", &[f(0.5)]), 1.0, 0.01);
    }
}

// ===========================================================================
// trim — 20 tests
// ===========================================================================
mod trim_f01 {
    use super::*;
    #[test]
    fn t00() {
        assert_eq!(ev("trim", &[s("  hello  ")]), s("hello"));
    }
    #[test]
    fn t01() {
        assert_eq!(ev("trim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn t02() {
        assert_eq!(ev("trim", &[s("")]), s(""));
    }
    #[test]
    fn t03() {
        assert_eq!(ev("trim", &[s("   ")]), s(""));
    }
    #[test]
    fn t04() {
        assert_eq!(ev("trim", &[s(" a ")]), s("a"));
    }
    #[test]
    fn t05() {
        assert_eq!(ev("trim", &[s("  ab  ")]), s("ab"));
    }
    #[test]
    fn t06() {
        assert_eq!(ev("trim", &[s("  abc  ")]), s("abc"));
    }
    #[test]
    fn t07() {
        assert_eq!(ev("trim", &[null()]), null());
    }
    #[test]
    fn t08() {
        assert_eq!(ev("ltrim", &[s("  hello")]), s("hello"));
    }
    #[test]
    fn t09() {
        assert_eq!(ev("ltrim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn t10() {
        assert_eq!(ev("ltrim", &[s("  hello  ")]), s("hello  "));
    }
    #[test]
    fn t11() {
        assert_eq!(ev("ltrim", &[null()]), null());
    }
    #[test]
    fn t12() {
        assert_eq!(ev("rtrim", &[s("hello  ")]), s("hello"));
    }
    #[test]
    fn t13() {
        assert_eq!(ev("rtrim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn t14() {
        assert_eq!(ev("rtrim", &[s("  hello  ")]), s("  hello"));
    }
    #[test]
    fn t15() {
        assert_eq!(ev("rtrim", &[null()]), null());
    }
    #[test]
    fn t16() {
        assert_eq!(ev("trim", &[s("\t hello \t")]), s("hello"));
    }
    #[test]
    fn t17() {
        assert_eq!(ev("ltrim", &[s("")]), s(""));
    }
    #[test]
    fn t18() {
        assert_eq!(ev("rtrim", &[s("")]), s(""));
    }
    #[test]
    fn t19() {
        assert_eq!(ev("trim", &[s(" x y z ")]), s("x y z"));
    }
}

// ===========================================================================
// concat — 20 tests
// ===========================================================================
mod concat_f01 {
    use super::*;
    #[test]
    fn c00() {
        assert_eq!(ev("concat", &[s("a"), s("b")]), s("ab"));
    }
    #[test]
    fn c01() {
        assert_eq!(
            ev("concat", &[s("hello"), s(" "), s("world")]),
            s("hello world")
        );
    }
    #[test]
    fn c02() {
        assert_eq!(ev("concat", &[s(""), s("a")]), s("a"));
    }
    #[test]
    fn c03() {
        assert_eq!(ev("concat", &[s("a"), s("")]), s("a"));
    }
    #[test]
    fn c04() {
        assert_eq!(ev("concat", &[s(""), s("")]), s(""));
    }
    #[test]
    fn c05() {
        assert_eq!(ev("concat", &[s("abc"), s("def")]), s("abcdef"));
    }
    #[test]
    fn c06() {
        assert_eq!(ev("concat", &[s("x"), s("y"), s("z")]), s("xyz"));
    }
    #[test]
    fn c07() {
        assert_eq!(ev("concat", &[s("1"), s("2"), s("3"), s("4")]), s("1234"));
    }
    #[test]
    fn c08() {
        assert_eq!(ev("concat", &[s("foo"), s("bar")]), s("foobar"));
    }
    #[test]
    fn c09() {
        assert_eq!(
            ev("concat", &[s("test"), s("_"), s("case")]),
            s("test_case")
        );
    }
    #[test]
    fn c10() {
        assert_eq!(
            ev("concat", &[s("A"), s("B"), s("C"), s("D"), s("E")]),
            s("ABCDE")
        );
    }
    #[test]
    fn c11() {
        assert_eq!(ev("concat", &[s("hello"), s("")]), s("hello"));
    }
    #[test]
    fn c12() {
        assert_eq!(ev("concat", &[s(""), s("world")]), s("world"));
    }
    #[test]
    fn c13() {
        assert_eq!(ev("concat", &[s("a"), s("b"), s("c")]), s("abc"));
    }
    #[test]
    fn c14() {
        assert_eq!(ev("concat", &[s("foo"), s(" "), s("bar")]), s("foo bar"));
    }
    #[test]
    fn c15() {
        assert_eq!(ev("concat", &[s("one"), s("two")]), s("onetwo"));
    }
    #[test]
    fn c16() {
        assert_eq!(ev("concat", &[s("pre"), s("fix")]), s("prefix"));
    }
    #[test]
    fn c17() {
        assert_eq!(ev("concat", &[s("suf"), s("fix")]), s("suffix"));
    }
    #[test]
    fn c18() {
        assert_eq!(ev("concat", &[s("data"), s("base")]), s("database"));
    }
    #[test]
    fn c19() {
        assert_eq!(ev("concat", &[s("time"), s("stamp")]), s("timestamp"));
    }
}

// ===========================================================================
// replace — 20 tests
// ===========================================================================
mod replace_f01 {
    use super::*;
    #[test]
    fn r00() {
        assert_eq!(ev("replace", &[s("hello"), s("l"), s("r")]), s("herro"));
    }
    #[test]
    fn r01() {
        assert_eq!(ev("replace", &[s("aaa"), s("a"), s("b")]), s("bbb"));
    }
    #[test]
    fn r02() {
        assert_eq!(ev("replace", &[s("abc"), s("b"), s("")]), s("ac"));
    }
    #[test]
    fn r03() {
        assert_eq!(ev("replace", &[s("abc"), s("x"), s("y")]), s("abc"));
    }
    #[test]
    fn r04() {
        assert_eq!(ev("replace", &[s(""), s("a"), s("b")]), s(""));
    }
    #[test]
    fn r05() {
        assert_eq!(
            ev("replace", &[s("hello world"), s(" "), s("_")]),
            s("hello_world")
        );
    }
    #[test]
    fn r06() {
        assert_eq!(ev("replace", &[s("aabbcc"), s("bb"), s("XX")]), s("aaXXcc"));
    }
    #[test]
    fn r07() {
        assert_eq!(ev("replace", &[null(), s("a"), s("b")]), null());
    }
    #[test]
    fn r08() {
        assert_eq!(ev("replace", &[s("test"), s("test"), s("done")]), s("done"));
    }
    #[test]
    fn r09() {
        assert_eq!(ev("replace", &[s("abcabc"), s("abc"), s("x")]), s("xx"));
    }
    #[test]
    fn r10() {
        assert_eq!(ev("replace", &[s("foo"), s("o"), s("0")]), s("f00"));
    }
    #[test]
    fn r11() {
        assert_eq!(ev("replace", &[s("bar"), s("bar"), s("baz")]), s("baz"));
    }
    #[test]
    fn r12() {
        assert_eq!(ev("replace", &[s("xxx"), s("x"), s("yy")]), s("yyyyyy"));
    }
    #[test]
    fn r13() {
        assert_eq!(ev("replace", &[s("hello"), s("hello"), s("")]), s(""));
    }
    #[test]
    fn r14() {
        assert_eq!(ev("replace", &[s("aaa"), s("aa"), s("b")]), s("ba"));
    }
    #[test]
    fn r15() {
        assert_eq!(
            ev("replace", &[s("12345"), s("3"), s("THREE")]),
            s("12THREE45")
        );
    }
    #[test]
    fn r16() {
        assert_eq!(ev("replace", &[s("abc"), s(""), s("x")]), s("abc"));
    }
    #[test]
    fn r17() {
        assert_eq!(
            ev("replace", &[s("HELLO"), s("HELLO"), s("hello")]),
            s("hello")
        );
    }
    #[test]
    fn r18() {
        assert_eq!(ev("replace", &[s("a.b.c"), s("."), s("-")]), s("a-b-c"));
    }
    #[test]
    fn r19() {
        assert_eq!(ev("replace", &[s("data"), s("data"), s("info")]), s("info"));
    }
}

// ===========================================================================
// reverse — 20 tests
// ===========================================================================
mod reverse_f01 {
    use super::*;
    #[test]
    fn r00() {
        assert_eq!(ev("reverse", &[s("abc")]), s("cba"));
    }
    #[test]
    fn r01() {
        assert_eq!(ev("reverse", &[s("hello")]), s("olleh"));
    }
    #[test]
    fn r02() {
        assert_eq!(ev("reverse", &[s("")]), s(""));
    }
    #[test]
    fn r03() {
        assert_eq!(ev("reverse", &[s("a")]), s("a"));
    }
    #[test]
    fn r04() {
        assert_eq!(ev("reverse", &[s("ab")]), s("ba"));
    }
    #[test]
    fn r05() {
        assert_eq!(ev("reverse", &[s("12345")]), s("54321"));
    }
    #[test]
    fn r06() {
        assert_eq!(ev("reverse", &[null()]), null());
    }
    #[test]
    fn r07() {
        assert_eq!(ev("reverse", &[s("racecar")]), s("racecar"));
    }
    #[test]
    fn r08() {
        assert_eq!(ev("reverse", &[s("madam")]), s("madam"));
    }
    #[test]
    fn r09() {
        assert_eq!(ev("reverse", &[s("level")]), s("level"));
    }
    #[test]
    fn r10() {
        assert_eq!(ev("reverse", &[s("abcdef")]), s("fedcba"));
    }
    #[test]
    fn r11() {
        assert_eq!(ev("reverse", &[s("xyz")]), s("zyx"));
    }
    #[test]
    fn r12() {
        assert_eq!(ev("reverse", &[s("test")]), s("tset"));
    }
    #[test]
    fn r13() {
        assert_eq!(ev("reverse", &[s("rust")]), s("tsur"));
    }
    #[test]
    fn r14() {
        assert_eq!(ev("reverse", &[s("data")]), s("atad"));
    }
    #[test]
    fn r15() {
        assert_eq!(ev("reverse", &[s("query")]), s("yreuq"));
    }
    #[test]
    fn r16() {
        assert_eq!(ev("reverse", &[s("table")]), s("elbat"));
    }
    #[test]
    fn r17() {
        assert_eq!(ev("reverse", &[s("index")]), s("xedni"));
    }
    #[test]
    fn r18() {
        assert_eq!(ev("reverse", &[s(" ")]), s(" "));
    }
    #[test]
    fn r19() {
        assert_eq!(ev("reverse", &[s("ab cd")]), s("dc ba"));
    }
}

// ===========================================================================
// contains — 20 tests
// ===========================================================================
mod contains_f01 {
    use super::*;
    #[test]
    fn c00() {
        assert_eq!(ev("contains", &[s("hello"), s("ell")]), i(1));
    }
    #[test]
    fn c01() {
        assert_eq!(ev("contains", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn c02() {
        assert_eq!(ev("contains", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn c03() {
        assert_eq!(ev("contains", &[s(""), s("")]), i(1));
    }
    #[test]
    fn c04() {
        assert_eq!(ev("contains", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn c05() {
        assert_eq!(ev("contains", &[s("hello"), s("hello")]), i(1));
    }
    #[test]
    fn c06() {
        assert_eq!(ev("contains", &[s("hello"), s("h")]), i(1));
    }
    #[test]
    fn c07() {
        assert_eq!(ev("contains", &[s("hello"), s("o")]), i(1));
    }
    #[test]
    fn c08() {
        assert_eq!(ev("contains", &[s("hello"), s("lo")]), i(1));
    }
    #[test]
    fn c09() {
        assert_eq!(ev("contains", &[s("hello"), s("he")]), i(1));
    }
    #[test]
    fn c10() {
        assert_eq!(ev("contains", &[null(), s("a")]), null());
    }
    #[test]
    fn c11() {
        assert_eq!(ev("contains", &[s("abcdef"), s("cd")]), i(1));
    }
    #[test]
    fn c12() {
        assert_eq!(ev("contains", &[s("abcdef"), s("ef")]), i(1));
    }
    #[test]
    fn c13() {
        assert_eq!(ev("contains", &[s("abcdef"), s("gh")]), i(0));
    }
    #[test]
    fn c14() {
        assert_eq!(ev("contains", &[s("abcdef"), s("ab")]), i(1));
    }
    #[test]
    fn c15() {
        assert_eq!(ev("contains", &[s("aaa"), s("aa")]), i(1));
    }
    #[test]
    fn c16() {
        assert_eq!(ev("contains", &[s("abc"), s("ABC")]), i(0));
    }
    #[test]
    fn c17() {
        assert_eq!(ev("contains", &[s("HELLO"), s("HELLO")]), i(1));
    }
    #[test]
    fn c18() {
        assert_eq!(ev("contains", &[s("test"), s("es")]), i(1));
    }
    #[test]
    fn c19() {
        assert_eq!(ev("contains", &[s("test"), s("st")]), i(1));
    }
}

// ===========================================================================
// starts_with — 20 tests
// ===========================================================================
mod starts_with_f01 {
    use super::*;
    #[test]
    fn s00() {
        assert_eq!(ev("starts_with", &[s("hello"), s("he")]), i(1));
    }
    #[test]
    fn s01() {
        assert_eq!(ev("starts_with", &[s("hello"), s("lo")]), i(0));
    }
    #[test]
    fn s02() {
        assert_eq!(ev("starts_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn s03() {
        assert_eq!(ev("starts_with", &[s(""), s("")]), i(1));
    }
    #[test]
    fn s04() {
        assert_eq!(ev("starts_with", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn s05() {
        assert_eq!(ev("starts_with", &[s("hello"), s("hello")]), i(1));
    }
    #[test]
    fn s06() {
        assert_eq!(ev("starts_with", &[s("hello"), s("h")]), i(1));
    }
    #[test]
    fn s07() {
        assert_eq!(ev("starts_with", &[s("hello"), s("hel")]), i(1));
    }
    #[test]
    fn s08() {
        assert_eq!(ev("starts_with", &[s("hello"), s("hell")]), i(1));
    }
    #[test]
    fn s09() {
        assert_eq!(ev("starts_with", &[null(), s("a")]), null());
    }
    #[test]
    fn s10() {
        assert_eq!(ev("starts_with", &[s("abc"), s("a")]), i(1));
    }
    #[test]
    fn s11() {
        assert_eq!(ev("starts_with", &[s("abc"), s("b")]), i(0));
    }
    #[test]
    fn s12() {
        assert_eq!(ev("starts_with", &[s("abc"), s("ab")]), i(1));
    }
    #[test]
    fn s13() {
        assert_eq!(ev("starts_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn s14() {
        assert_eq!(ev("starts_with", &[s("abc"), s("abcd")]), i(0));
    }
    #[test]
    fn s15() {
        assert_eq!(ev("starts_with", &[s("xyz"), s("x")]), i(1));
    }
    #[test]
    fn s16() {
        assert_eq!(ev("starts_with", &[s("xyz"), s("xy")]), i(1));
    }
    #[test]
    fn s17() {
        assert_eq!(ev("starts_with", &[s("xyz"), s("xyz")]), i(1));
    }
    #[test]
    fn s18() {
        assert_eq!(ev("starts_with", &[s("xyz"), s("y")]), i(0));
    }
    #[test]
    fn s19() {
        assert_eq!(ev("starts_with", &[s("test"), s("te")]), i(1));
    }
}

// ===========================================================================
// ends_with — 20 tests
// ===========================================================================
mod ends_with_f01 {
    use super::*;
    #[test]
    fn e00() {
        assert_eq!(ev("ends_with", &[s("hello"), s("lo")]), i(1));
    }
    #[test]
    fn e01() {
        assert_eq!(ev("ends_with", &[s("hello"), s("he")]), i(0));
    }
    #[test]
    fn e02() {
        assert_eq!(ev("ends_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn e03() {
        assert_eq!(ev("ends_with", &[s(""), s("")]), i(1));
    }
    #[test]
    fn e04() {
        assert_eq!(ev("ends_with", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn e05() {
        assert_eq!(ev("ends_with", &[s("hello"), s("hello")]), i(1));
    }
    #[test]
    fn e06() {
        assert_eq!(ev("ends_with", &[s("hello"), s("o")]), i(1));
    }
    #[test]
    fn e07() {
        assert_eq!(ev("ends_with", &[s("hello"), s("llo")]), i(1));
    }
    #[test]
    fn e08() {
        assert_eq!(ev("ends_with", &[s("hello"), s("ello")]), i(1));
    }
    #[test]
    fn e09() {
        assert_eq!(ev("ends_with", &[null(), s("a")]), null());
    }
    #[test]
    fn e10() {
        assert_eq!(ev("ends_with", &[s("abc"), s("c")]), i(1));
    }
    #[test]
    fn e11() {
        assert_eq!(ev("ends_with", &[s("abc"), s("b")]), i(0));
    }
    #[test]
    fn e12() {
        assert_eq!(ev("ends_with", &[s("abc"), s("bc")]), i(1));
    }
    #[test]
    fn e13() {
        assert_eq!(ev("ends_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn e14() {
        assert_eq!(ev("ends_with", &[s("abc"), s("xabc")]), i(0));
    }
    #[test]
    fn e15() {
        assert_eq!(ev("ends_with", &[s("xyz"), s("z")]), i(1));
    }
    #[test]
    fn e16() {
        assert_eq!(ev("ends_with", &[s("xyz"), s("yz")]), i(1));
    }
    #[test]
    fn e17() {
        assert_eq!(ev("ends_with", &[s("xyz"), s("xyz")]), i(1));
    }
    #[test]
    fn e18() {
        assert_eq!(ev("ends_with", &[s("xyz"), s("x")]), i(0));
    }
    #[test]
    fn e19() {
        assert_eq!(ev("ends_with", &[s("test"), s("st")]), i(1));
    }
}

// ===========================================================================
// substring — 20 tests
// ===========================================================================
mod substring_f01 {
    use super::*;
    #[test]
    fn s00() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(3)]), s("hel"));
    }
    #[test]
    fn s01() {
        assert_eq!(ev("substring", &[s("hello"), i(2), i(3)]), s("ell"));
    }
    #[test]
    fn s02() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(5)]), s("hello"));
    }
    #[test]
    fn s03() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(1)]), s("h"));
    }
    #[test]
    fn s04() {
        assert_eq!(ev("substring", &[s("hello"), i(5), i(1)]), s("o"));
    }
    #[test]
    fn s05() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(0)]), s(""));
    }
    #[test]
    fn s06() {
        assert_eq!(ev("substring", &[null(), i(1), i(1)]), null());
    }
    #[test]
    fn s07() {
        assert_eq!(ev("substring", &[s("abcdef"), i(1), i(3)]), s("abc"));
    }
    #[test]
    fn s08() {
        assert_eq!(ev("substring", &[s("abcdef"), i(4), i(3)]), s("def"));
    }
    #[test]
    fn s09() {
        assert_eq!(ev("substring", &[s("abcdef"), i(2), i(4)]), s("bcde"));
    }
    #[test]
    fn s10() {
        assert_eq!(ev("substring", &[s("abcdef"), i(1), i(6)]), s("abcdef"));
    }
    #[test]
    fn s11() {
        assert_eq!(ev("substring", &[s("test"), i(1), i(2)]), s("te"));
    }
    #[test]
    fn s12() {
        assert_eq!(ev("substring", &[s("test"), i(3), i(2)]), s("st"));
    }
    #[test]
    fn s13() {
        assert_eq!(ev("substring", &[s("test"), i(2), i(2)]), s("es"));
    }
    #[test]
    fn s14() {
        assert_eq!(ev("substring", &[s("rust"), i(1), i(4)]), s("rust"));
    }
    #[test]
    fn s15() {
        assert_eq!(ev("substring", &[s("rust"), i(1), i(2)]), s("ru"));
    }
    #[test]
    fn s16() {
        assert_eq!(ev("substring", &[s("rust"), i(3), i(2)]), s("st"));
    }
    #[test]
    fn s17() {
        assert_eq!(ev("substring", &[s("data"), i(1), i(4)]), s("data"));
    }
    #[test]
    fn s18() {
        assert_eq!(ev("substring", &[s("data"), i(1), i(2)]), s("da"));
    }
    #[test]
    fn s19() {
        assert_eq!(ev("substring", &[s("data"), i(3), i(2)]), s("ta"));
    }
}

// ===========================================================================
// left — 20 tests
// ===========================================================================
mod left_f01 {
    use super::*;
    #[test]
    fn l00() {
        assert_eq!(ev("left", &[s("hello"), i(3)]), s("hel"));
    }
    #[test]
    fn l01() {
        assert_eq!(ev("left", &[s("hello"), i(5)]), s("hello"));
    }
    #[test]
    fn l02() {
        assert_eq!(ev("left", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn l03() {
        assert_eq!(ev("left", &[s("hello"), i(1)]), s("h"));
    }
    #[test]
    fn l04() {
        assert_eq!(ev("left", &[s("hello"), i(2)]), s("he"));
    }
    #[test]
    fn l05() {
        assert_eq!(ev("left", &[s("hello"), i(4)]), s("hell"));
    }
    #[test]
    fn l06() {
        assert_eq!(ev("left", &[null(), i(3)]), null());
    }
    #[test]
    fn l07() {
        assert_eq!(ev("left", &[s("abc"), i(1)]), s("a"));
    }
    #[test]
    fn l08() {
        assert_eq!(ev("left", &[s("abc"), i(2)]), s("ab"));
    }
    #[test]
    fn l09() {
        assert_eq!(ev("left", &[s("abc"), i(3)]), s("abc"));
    }
    #[test]
    fn l10() {
        assert_eq!(ev("left", &[s("abcdef"), i(4)]), s("abcd"));
    }
    #[test]
    fn l11() {
        assert_eq!(ev("left", &[s(""), i(0)]), s(""));
    }
    #[test]
    fn l12() {
        assert_eq!(ev("left", &[s("test"), i(1)]), s("t"));
    }
    #[test]
    fn l13() {
        assert_eq!(ev("left", &[s("test"), i(2)]), s("te"));
    }
    #[test]
    fn l14() {
        assert_eq!(ev("left", &[s("test"), i(3)]), s("tes"));
    }
    #[test]
    fn l15() {
        assert_eq!(ev("left", &[s("test"), i(4)]), s("test"));
    }
    #[test]
    fn l16() {
        assert_eq!(ev("left", &[s("rust"), i(1)]), s("r"));
    }
    #[test]
    fn l17() {
        assert_eq!(ev("left", &[s("rust"), i(2)]), s("ru"));
    }
    #[test]
    fn l18() {
        assert_eq!(ev("left", &[s("rust"), i(3)]), s("rus"));
    }
    #[test]
    fn l19() {
        assert_eq!(ev("left", &[s("rust"), i(4)]), s("rust"));
    }
}

// ===========================================================================
// right — 20 tests
// ===========================================================================
mod right_f01 {
    use super::*;
    #[test]
    fn r00() {
        assert_eq!(ev("right", &[s("hello"), i(3)]), s("llo"));
    }
    #[test]
    fn r01() {
        assert_eq!(ev("right", &[s("hello"), i(5)]), s("hello"));
    }
    #[test]
    fn r02() {
        assert_eq!(ev("right", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn r03() {
        assert_eq!(ev("right", &[s("hello"), i(1)]), s("o"));
    }
    #[test]
    fn r04() {
        assert_eq!(ev("right", &[s("hello"), i(2)]), s("lo"));
    }
    #[test]
    fn r05() {
        assert_eq!(ev("right", &[s("hello"), i(4)]), s("ello"));
    }
    #[test]
    fn r06() {
        assert_eq!(ev("right", &[null(), i(3)]), null());
    }
    #[test]
    fn r07() {
        assert_eq!(ev("right", &[s("abc"), i(1)]), s("c"));
    }
    #[test]
    fn r08() {
        assert_eq!(ev("right", &[s("abc"), i(2)]), s("bc"));
    }
    #[test]
    fn r09() {
        assert_eq!(ev("right", &[s("abc"), i(3)]), s("abc"));
    }
    #[test]
    fn r10() {
        assert_eq!(ev("right", &[s("abcdef"), i(4)]), s("cdef"));
    }
    #[test]
    fn r11() {
        assert_eq!(ev("right", &[s(""), i(0)]), s(""));
    }
    #[test]
    fn r12() {
        assert_eq!(ev("right", &[s("test"), i(1)]), s("t"));
    }
    #[test]
    fn r13() {
        assert_eq!(ev("right", &[s("test"), i(2)]), s("st"));
    }
    #[test]
    fn r14() {
        assert_eq!(ev("right", &[s("test"), i(3)]), s("est"));
    }
    #[test]
    fn r15() {
        assert_eq!(ev("right", &[s("test"), i(4)]), s("test"));
    }
    #[test]
    fn r16() {
        assert_eq!(ev("right", &[s("rust"), i(1)]), s("t"));
    }
    #[test]
    fn r17() {
        assert_eq!(ev("right", &[s("rust"), i(2)]), s("st"));
    }
    #[test]
    fn r18() {
        assert_eq!(ev("right", &[s("rust"), i(3)]), s("ust"));
    }
    #[test]
    fn r19() {
        assert_eq!(ev("right", &[s("rust"), i(4)]), s("rust"));
    }
}

// ===========================================================================
// position — 20 tests
// ===========================================================================
mod position_f01 {
    use super::*;
    // position(needle, haystack) -> 1-based index
    #[test]
    fn p00() {
        assert_eq!(ev("position", &[s("ell"), s("hello")]), i(2));
    }
    #[test]
    fn p01() {
        assert_eq!(ev("position", &[s("h"), s("hello")]), i(1));
    }
    #[test]
    fn p02() {
        assert_eq!(ev("position", &[s("o"), s("hello")]), i(5));
    }
    #[test]
    fn p03() {
        assert_eq!(ev("position", &[s("xyz"), s("hello")]), i(0));
    }
    #[test]
    fn p04() {
        assert_eq!(ev("position", &[s(""), s("hello")]), i(1));
    }
    #[test]
    fn p05() {
        assert_eq!(ev("position", &[s(""), s("")]), i(1));
    }
    #[test]
    fn p06() {
        assert_eq!(ev("position", &[s("a"), s("")]), i(0));
    }
    #[test]
    fn p07() {
        assert_eq!(ev("position", &[null(), s("a")]), null());
    }
    #[test]
    fn p08() {
        assert_eq!(ev("position", &[s("cd"), s("abcdef")]), i(3));
    }
    #[test]
    fn p09() {
        assert_eq!(ev("position", &[s("ef"), s("abcdef")]), i(5));
    }
    #[test]
    fn p10() {
        assert_eq!(ev("position", &[s("ab"), s("abcdef")]), i(1));
    }
    #[test]
    fn p11() {
        assert_eq!(ev("position", &[s("f"), s("abcdef")]), i(6));
    }
    #[test]
    fn p12() {
        assert_eq!(ev("position", &[s("aa"), s("aaa")]), i(1));
    }
    #[test]
    fn p13() {
        assert_eq!(ev("position", &[s("es"), s("test")]), i(2));
    }
    #[test]
    fn p14() {
        assert_eq!(ev("position", &[s("st"), s("test")]), i(3));
    }
    #[test]
    fn p15() {
        assert_eq!(ev("position", &[s("t"), s("test")]), i(1));
    }
    #[test]
    fn p16() {
        assert_eq!(ev("position", &[s("r"), s("rust")]), i(1));
    }
    #[test]
    fn p17() {
        assert_eq!(ev("position", &[s("u"), s("rust")]), i(2));
    }
    #[test]
    fn p18() {
        assert_eq!(ev("position", &[s("s"), s("rust")]), i(3));
    }
    #[test]
    fn p19() {
        assert_eq!(ev("position", &[s("t"), s("rust")]), i(4));
    }
}

// ===========================================================================
// repeat — 20 tests
// ===========================================================================
mod repeat_f01 {
    use super::*;
    #[test]
    fn r00() {
        assert_eq!(ev("repeat", &[s("a"), i(3)]), s("aaa"));
    }
    #[test]
    fn r01() {
        assert_eq!(ev("repeat", &[s("ab"), i(2)]), s("abab"));
    }
    #[test]
    fn r02() {
        assert_eq!(ev("repeat", &[s("x"), i(0)]), s(""));
    }
    #[test]
    fn r03() {
        assert_eq!(ev("repeat", &[s("x"), i(1)]), s("x"));
    }
    #[test]
    fn r04() {
        assert_eq!(ev("repeat", &[s("x"), i(5)]), s("xxxxx"));
    }
    #[test]
    fn r05() {
        assert_eq!(ev("repeat", &[s("x"), i(10)]), s("xxxxxxxxxx"));
    }
    #[test]
    fn r06() {
        assert_eq!(ev("repeat", &[s(""), i(5)]), s(""));
    }
    #[test]
    fn r07() {
        assert_eq!(ev("repeat", &[null(), i(3)]), null());
    }
    #[test]
    fn r08() {
        assert_eq!(ev("repeat", &[s("abc"), i(1)]), s("abc"));
    }
    #[test]
    fn r09() {
        assert_eq!(ev("repeat", &[s("abc"), i(2)]), s("abcabc"));
    }
    #[test]
    fn r10() {
        assert_eq!(ev("repeat", &[s("abc"), i(3)]), s("abcabcabc"));
    }
    #[test]
    fn r11() {
        assert_eq!(ev("repeat", &[s("hi"), i(4)]), s("hihihihi"));
    }
    #[test]
    fn r12() {
        assert_eq!(ev("repeat", &[s("z"), i(7)]), s("zzzzzzz"));
    }
    #[test]
    fn r13() {
        assert_eq!(ev("repeat", &[s(" "), i(3)]), s("   "));
    }
    #[test]
    fn r14() {
        assert_eq!(ev("repeat", &[s("12"), i(3)]), s("121212"));
    }
    #[test]
    fn r15() {
        assert_eq!(ev("repeat", &[s("ab"), i(5)]), s("ababababab"));
    }
    #[test]
    fn r16() {
        assert_eq!(ev("repeat", &[s("xy"), i(0)]), s(""));
    }
    #[test]
    fn r17() {
        assert_eq!(ev("repeat", &[s("a"), i(20)]), s("aaaaaaaaaaaaaaaaaaaa"));
    }
    #[test]
    fn r18() {
        assert_eq!(ev("repeat", &[s("b"), i(2)]), s("bb"));
    }
    #[test]
    fn r19() {
        assert_eq!(ev("repeat", &[s("cd"), i(4)]), s("cdcdcdcd"));
    }
}

// ===========================================================================
// lpad — 20 tests
// ===========================================================================
mod lpad_f01 {
    use super::*;
    #[test]
    fn l00() {
        assert_eq!(ev("lpad", &[s("hi"), i(5), s(" ")]), s("   hi"));
    }
    #[test]
    fn l01() {
        assert_eq!(ev("lpad", &[s("hi"), i(5), s("0")]), s("000hi"));
    }
    #[test]
    fn l02() {
        assert_eq!(ev("lpad", &[s("hi"), i(2), s(" ")]), s("hi"));
    }
    #[test]
    fn l03() {
        assert_eq!(ev("lpad", &[s("hi"), i(1), s(" ")]), s("h"));
    }
    #[test]
    fn l04() {
        assert_eq!(ev("lpad", &[s(""), i(3), s("x")]), s("xxx"));
    }
    #[test]
    fn l05() {
        assert_eq!(ev("lpad", &[null(), i(5), s(" ")]), null());
    }
    #[test]
    fn l06() {
        assert_eq!(ev("lpad", &[s("abc"), i(6), s("0")]), s("000abc"));
    }
    #[test]
    fn l07() {
        assert_eq!(ev("lpad", &[s("abc"), i(3), s("0")]), s("abc"));
    }
    #[test]
    fn l08() {
        assert_eq!(ev("lpad", &[s("a"), i(5), s("_")]), s("____a"));
    }
    #[test]
    fn l09() {
        assert_eq!(ev("lpad", &[s("test"), i(8), s(".")]), s("....test"));
    }
    #[test]
    fn l10() {
        assert_eq!(ev("lpad", &[s("x"), i(1), s(" ")]), s("x"));
    }
    #[test]
    fn l11() {
        assert_eq!(ev("lpad", &[s("x"), i(3), s("-")]), s("--x"));
    }
    #[test]
    fn l12() {
        assert_eq!(ev("lpad", &[s("ab"), i(4), s("0")]), s("00ab"));
    }
    #[test]
    fn l13() {
        assert_eq!(ev("lpad", &[s(""), i(0), s("x")]), s(""));
    }
    #[test]
    fn l14() {
        assert_eq!(ev("lpad", &[s("z"), i(4), s("z")]), s("zzzz"));
    }
    #[test]
    fn l15() {
        assert_eq!(ev("lpad", &[s("12"), i(5), s("0")]), s("00012"));
    }
    #[test]
    fn l16() {
        assert_eq!(ev("lpad", &[s("1"), i(3), s("0")]), s("001"));
    }
    #[test]
    fn l17() {
        assert_eq!(ev("lpad", &[s("1"), i(4), s("0")]), s("0001"));
    }
    #[test]
    fn l18() {
        assert_eq!(ev("lpad", &[s("1"), i(5), s("0")]), s("00001"));
    }
    #[test]
    fn l19() {
        assert_eq!(ev("lpad", &[s("99"), i(4), s("0")]), s("0099"));
    }
}

// ===========================================================================
// rpad — 20 tests
// ===========================================================================
mod rpad_f01 {
    use super::*;
    #[test]
    fn r00() {
        assert_eq!(ev("rpad", &[s("hi"), i(5), s(" ")]), s("hi   "));
    }
    #[test]
    fn r01() {
        assert_eq!(ev("rpad", &[s("hi"), i(5), s("0")]), s("hi000"));
    }
    #[test]
    fn r02() {
        assert_eq!(ev("rpad", &[s("hi"), i(2), s(" ")]), s("hi"));
    }
    #[test]
    fn r03() {
        assert_eq!(ev("rpad", &[s("hi"), i(1), s(" ")]), s("h"));
    }
    #[test]
    fn r04() {
        assert_eq!(ev("rpad", &[s(""), i(3), s("x")]), s("xxx"));
    }
    #[test]
    fn r05() {
        assert_eq!(ev("rpad", &[null(), i(5), s(" ")]), null());
    }
    #[test]
    fn r06() {
        assert_eq!(ev("rpad", &[s("abc"), i(6), s("0")]), s("abc000"));
    }
    #[test]
    fn r07() {
        assert_eq!(ev("rpad", &[s("abc"), i(3), s("0")]), s("abc"));
    }
    #[test]
    fn r08() {
        assert_eq!(ev("rpad", &[s("a"), i(5), s("_")]), s("a____"));
    }
    #[test]
    fn r09() {
        assert_eq!(ev("rpad", &[s("test"), i(8), s(".")]), s("test...."));
    }
    #[test]
    fn r10() {
        assert_eq!(ev("rpad", &[s("x"), i(1), s(" ")]), s("x"));
    }
    #[test]
    fn r11() {
        assert_eq!(ev("rpad", &[s("x"), i(3), s("-")]), s("x--"));
    }
    #[test]
    fn r12() {
        assert_eq!(ev("rpad", &[s("ab"), i(4), s("0")]), s("ab00"));
    }
    #[test]
    fn r13() {
        assert_eq!(ev("rpad", &[s(""), i(0), s("x")]), s(""));
    }
    #[test]
    fn r14() {
        assert_eq!(ev("rpad", &[s("z"), i(4), s("z")]), s("zzzz"));
    }
    #[test]
    fn r15() {
        assert_eq!(ev("rpad", &[s("12"), i(5), s("0")]), s("12000"));
    }
    #[test]
    fn r16() {
        assert_eq!(ev("rpad", &[s("1"), i(3), s("0")]), s("100"));
    }
    #[test]
    fn r17() {
        assert_eq!(ev("rpad", &[s("1"), i(4), s("0")]), s("1000"));
    }
    #[test]
    fn r18() {
        assert_eq!(ev("rpad", &[s("1"), i(5), s("0")]), s("10000"));
    }
    #[test]
    fn r19() {
        assert_eq!(ev("rpad", &[s("99"), i(4), s("0")]), s("9900"));
    }
}

// ===========================================================================
// power/log/exp — 20 tests
// ===========================================================================
mod math_extra_f01 {
    use super::*;
    #[test]
    fn pow00() {
        close(&ev("power", &[f(2.0), f(3.0)]), 8.0, 0.01);
    }
    #[test]
    fn pow01() {
        close(&ev("power", &[f(3.0), f(2.0)]), 9.0, 0.01);
    }
    #[test]
    fn pow02() {
        close(&ev("power", &[f(10.0), f(0.0)]), 1.0, 0.01);
    }
    #[test]
    fn pow03() {
        close(&ev("power", &[f(10.0), f(1.0)]), 10.0, 0.01);
    }
    #[test]
    fn pow04() {
        close(&ev("power", &[f(2.0), f(10.0)]), 1024.0, 0.01);
    }
    #[test]
    fn pow05() {
        close(&ev("power", &[i(2), i(8)]), 256.0, 0.01);
    }
    #[test]
    fn pow06() {
        close(&ev("power", &[i(3), i(3)]), 27.0, 0.01);
    }
    #[test]
    fn pow07() {
        close(&ev("power", &[i(5), i(2)]), 25.0, 0.01);
    }
    #[test]
    fn pow08() {
        close(&ev("power", &[i(4), i(3)]), 64.0, 0.01);
    }
    #[test]
    fn pow09() {
        assert_eq!(ev("power", &[null(), f(2.0)]), null());
    }
    #[test]
    fn exp00() {
        close(&ev("exp", &[f(0.0)]), 1.0, 0.01);
    }
    #[test]
    fn exp01() {
        close(&ev("exp", &[f(1.0)]), std::f64::consts::E, 0.01);
    }
    #[test]
    fn exp02() {
        close(&ev("exp", &[i(0)]), 1.0, 0.01);
    }
    #[test]
    fn exp03() {
        assert_eq!(ev("exp", &[null()]), null());
    }
    #[test]
    fn log00() {
        close(&ev("ln", &[f(1.0)]), 0.0, 0.01);
    }
    #[test]
    fn log01() {
        close(&ev("ln", &[f(std::f64::consts::E)]), 1.0, 0.01);
    }
    #[test]
    fn log02() {
        assert_eq!(ev("ln", &[null()]), null());
    }
    #[test]
    fn log10_00() {
        close(&ev("log10", &[f(100.0)]), 2.0, 0.01);
    }
    #[test]
    fn log10_01() {
        close(&ev("log10", &[f(1000.0)]), 3.0, 0.01);
    }
    #[test]
    fn log10_02() {
        close(&ev("log10", &[f(1.0)]), 0.0, 0.01);
    }
}

// ===========================================================================
// coalesce/nullif/if — 20 tests
// ===========================================================================
mod conditional_f01 {
    use super::*;
    #[test]
    fn coal00() {
        assert_eq!(ev("coalesce", &[null(), i(1)]), i(1));
    }
    #[test]
    fn coal01() {
        assert_eq!(ev("coalesce", &[i(5), i(1)]), i(5));
    }
    #[test]
    fn coal02() {
        assert_eq!(ev("coalesce", &[null(), null(), i(3)]), i(3));
    }
    #[test]
    fn coal03() {
        assert_eq!(ev("coalesce", &[null(), null(), null()]), null());
    }
    #[test]
    fn coal04() {
        assert_eq!(ev("coalesce", &[s("a"), s("b")]), s("a"));
    }
    #[test]
    fn coal05() {
        assert_eq!(ev("coalesce", &[null(), s("b")]), s("b"));
    }
    #[test]
    fn coal06() {
        assert_eq!(ev("coalesce", &[i(0), i(1)]), i(0));
    }
    #[test]
    fn coal07() {
        assert_eq!(ev("coalesce", &[null(), f(3.15)]), f(3.15));
    }
    #[test]
    fn coal08() {
        assert_eq!(ev("coalesce", &[f(1.0), null()]), f(1.0));
    }
    #[test]
    fn coal09() {
        assert_eq!(ev("coalesce", &[null(), null(), null(), i(42)]), i(42));
    }
    #[test]
    fn nif00() {
        assert_eq!(ev("nullif", &[i(1), i(1)]), null());
    }
    #[test]
    fn nif01() {
        assert_eq!(ev("nullif", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn nif02() {
        assert_eq!(ev("nullif", &[s("a"), s("a")]), null());
    }
    #[test]
    fn nif03() {
        assert_eq!(ev("nullif", &[s("a"), s("b")]), s("a"));
    }
    #[test]
    fn nif04() {
        assert_eq!(ev("nullif", &[i(0), i(0)]), null());
    }
    #[test]
    fn nif05() {
        assert_eq!(ev("nullif", &[i(0), i(1)]), i(0));
    }
    #[test]
    fn iif00() {
        assert_eq!(ev("iif", &[i(1), s("yes"), s("no")]), s("yes"));
    }
    #[test]
    fn iif01() {
        assert_eq!(ev("iif", &[i(0), s("yes"), s("no")]), s("no"));
    }
    #[test]
    fn iif02() {
        assert_eq!(ev("iif", &[null(), s("yes"), s("no")]), s("no"));
    }
    #[test]
    fn iif03() {
        assert_eq!(ev("iif", &[i(1), i(10), i(20)]), i(10));
    }
}
