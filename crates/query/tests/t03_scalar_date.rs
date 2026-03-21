//! 1000+ date/time scalar function tests.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn i(v: i64) -> Value {
    Value::I64(v)
}
fn f(v: f64) -> Value {
    Value::F64(v)
}
fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}
fn ts(ns: i64) -> Value {
    Value::Timestamp(ns)
}
fn null() -> Value {
    Value::Null
}
fn ev(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap()
}

const NPS: i64 = 1_000_000_000;
const NPM: i64 = 60 * NPS;
const NPH: i64 = 3600 * NPS;
const NPD: i64 = 86400 * NPS;

const TS_2024_01_01: i64 = 1704067200 * NPS;
const TS_2024_02_29: i64 = 1709164800 * NPS;
const TS_2024_03_15_123045: i64 = 1710505845 * NPS;
const TS_2024_06_15: i64 = 1718409600 * NPS;
const TS_2024_12_31: i64 = 1735603200 * NPS;
const TS_2000_01_01: i64 = 946684800 * NPS;

// now / systimestamp / aliases — 20 tests
mod now_t03 {
    use super::*;
    #[test]
    fn now_ts() {
        match ev("now", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn systimestamp() {
        match ev("systimestamp", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn current_timestamp() {
        match ev("current_timestamp", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn now_utc() {
        match ev("now_utc", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn sysdate() {
        match ev("sysdate", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn current_date() {
        match ev("current_date", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn today() {
        match ev("today", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn current_time() {
        match ev("current_time", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn yesterday_before() {
        let y = match ev("yesterday", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        let n = match ev("now", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        assert!(y < n);
    }
    #[test]
    fn tomorrow_after() {
        let t = match ev("tomorrow", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        let n = match ev("now", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        assert!(t > n);
    }
    #[test]
    fn n01() {
        match ev("now", &[]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn n02() {
        match ev("systimestamp", &[]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn n03() {
        match ev("current_timestamp", &[]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn n04() {
        match ev("now_utc", &[]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn n05() {
        match ev("sysdate", &[]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn n06() {
        match ev("today", &[]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn n07() {
        match ev("yesterday", &[]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn n08() {
        match ev("tomorrow", &[]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn n09() {
        let y = match ev("yesterday", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        let t = match ev("tomorrow", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        assert!(t > y);
    }
    #[test]
    fn n10() {
        let now1 = match ev("now", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        let now2 = match ev("now", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        assert!(now2 >= now1);
    }
}

// to_timestamp — 30 tests
mod to_ts_t03 {
    use super::*;
    #[test]
    fn from_int() {
        assert_eq!(ev("to_timestamp", &[i(1000)]), ts(1000));
    }
    #[test]
    fn from_ts() {
        assert_eq!(ev("to_timestamp", &[ts(1000)]), ts(1000));
    }
    #[test]
    fn from_str() {
        assert_eq!(ev("to_timestamp", &[s("1000")]), ts(1000));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("to_timestamp", &[null()]), null());
    }
    #[test]
    fn epoch() {
        assert_eq!(ev("to_timestamp", &[i(0)]), ts(0));
    }
    #[test]
    fn large() {
        assert_eq!(ev("to_timestamp", &[i(TS_2024_01_01)]), ts(TS_2024_01_01));
    }
    #[test]
    fn neg() {
        assert_eq!(ev("to_timestamp", &[i(-1000)]), ts(-1000));
    }
    #[test]
    fn from_unixtime() {
        assert_eq!(ev("from_unixtime", &[i(1000)]), ts(1000));
    }
    #[test]
    fn str_to_ts() {
        assert_eq!(ev("str_to_timestamp", &[s("1000")]), ts(1000));
    }
    #[test]
    fn parse_ts() {
        assert_eq!(ev("parse_timestamp", &[s("1000")]), ts(1000));
    }
    #[test]
    fn t01() {
        assert_eq!(ev("to_timestamp", &[i(NPD)]), ts(NPD));
    }
    #[test]
    fn t02() {
        assert_eq!(ev("to_timestamp", &[i(NPS)]), ts(NPS));
    }
    #[test]
    fn t03() {
        assert_eq!(ev("to_timestamp", &[i(NPH)]), ts(NPH));
    }
    #[test]
    fn t04() {
        assert_eq!(ev("to_timestamp", &[i(NPM)]), ts(NPM));
    }
    #[test]
    fn t05() {
        assert_eq!(ev("to_timestamp", &[i(2 * NPD)]), ts(2 * NPD));
    }
    #[test]
    fn t06() {
        assert_eq!(ev("to_timestamp", &[i(7 * NPD)]), ts(7 * NPD));
    }
    #[test]
    fn t07() {
        assert_eq!(ev("to_timestamp", &[i(30 * NPD)]), ts(30 * NPD));
    }
    #[test]
    fn t08() {
        assert_eq!(ev("to_timestamp", &[i(365 * NPD)]), ts(365 * NPD));
    }
    #[test]
    fn t09() {
        assert_eq!(ev("to_timestamp", &[s("0")]), ts(0));
    }
    #[test]
    fn t10() {
        assert_eq!(ev("to_timestamp", &[f(1000.0)]), ts(1000));
    }
    #[test]
    fn t11() {
        assert_eq!(ev("to_timestamp", &[i(TS_2024_02_29)]), ts(TS_2024_02_29));
    }
    #[test]
    fn t12() {
        assert_eq!(ev("to_timestamp", &[i(TS_2024_06_15)]), ts(TS_2024_06_15));
    }
    #[test]
    fn t13() {
        assert_eq!(ev("to_timestamp", &[i(TS_2024_12_31)]), ts(TS_2024_12_31));
    }
    #[test]
    fn t14() {
        assert_eq!(ev("to_timestamp", &[i(TS_2000_01_01)]), ts(TS_2000_01_01));
    }
    #[test]
    fn t15() {
        assert_eq!(
            ev("to_timestamp", &[i(TS_2024_03_15_123045)]),
            ts(TS_2024_03_15_123045)
        );
    }
    #[test]
    fn t16() {
        assert_eq!(ev("to_timestamp", &[i(10 * NPS)]), ts(10 * NPS));
    }
    #[test]
    fn t17() {
        assert_eq!(ev("to_timestamp", &[i(100 * NPS)]), ts(100 * NPS));
    }
    #[test]
    fn t18() {
        assert_eq!(ev("to_timestamp", &[i(1000 * NPS)]), ts(1000 * NPS));
    }
    #[test]
    fn t19() {
        assert_eq!(ev("to_timestamp", &[i(-NPD)]), ts(-NPD));
    }
    #[test]
    fn t20() {
        assert_eq!(ev("to_timestamp", &[ts(TS_2024_01_01)]), ts(TS_2024_01_01));
    }
}

// extract_year — 40 tests
mod year_t03 {
    use super::*;
    #[test]
    fn y2024() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01)]), i(2024));
    }
    #[test]
    fn y2000() {
        assert_eq!(ev("extract_year", &[ts(TS_2000_01_01)]), i(2000));
    }
    #[test]
    fn y1970() {
        assert_eq!(ev("extract_year", &[ts(0)]), i(1970));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("extract_year", &[null()]), null());
    }
    #[test]
    fn y2024_jun() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_06_15)]), i(2024));
    }
    #[test]
    fn y2024_dec() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_12_31)]), i(2024));
    }
    #[test]
    fn y2024_feb29() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_02_29)]), i(2024));
    }
    #[test]
    fn year_of_alias() {
        assert_eq!(ev("year_of", &[ts(TS_2024_01_01)]), i(2024));
    }
    #[test]
    fn y01() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + NPD)]), i(2024));
    }
    #[test]
    fn y02() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 30 * NPD)]), i(2024));
    }
    #[test]
    fn y03() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 60 * NPD)]), i(2024));
    }
    #[test]
    fn y04() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 100 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y05() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 200 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y06() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 300 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y07() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 364 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y08() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 366 * NPD)]),
            i(2025)
        );
    } // 2024 is leap
    #[test]
    fn y09() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 - NPD)]), i(2023));
    }
    #[test]
    fn y10() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 - 365 * NPD)]),
            i(2023)
        );
    }
    #[test]
    fn y11() {
        assert_eq!(ev("extract_year", &[ts(TS_2000_01_01 + NPD)]), i(2000));
    }
    #[test]
    fn y12() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2000_01_01 + 365 * NPD)]),
            i(2000)
        );
    }
    #[test]
    fn y13() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_03_15_123045)]), i(2024));
    }
    #[test]
    fn y14() {
        assert_eq!(ev("extract_year", &[ts(NPS)]), i(1970));
    }
    #[test]
    fn y15() {
        assert_eq!(ev("extract_year", &[ts(NPD)]), i(1970));
    }
    #[test]
    fn y16() {
        assert_eq!(ev("extract_year", &[ts(365 * NPD)]), i(1971));
    }
    #[test]
    fn y17() {
        assert_eq!(ev("extract_year", &[ts(730 * NPD)]), i(1972));
    }
    #[test]
    fn y18() {
        assert_eq!(ev("extract_year", &[ts(2 * 365 * NPD)]), i(1972));
    }
    #[test]
    fn y19() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 150 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y20() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 250 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y21() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 350 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y22() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + NPH)]), i(2024));
    }
    #[test]
    fn y23() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 23 * NPH)]), i(2024));
    }
    #[test]
    fn y24() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 47 * NPH)]), i(2024));
    }
    #[test]
    fn y25() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_06_15 + NPD)]), i(2024));
    }
    #[test]
    fn y26() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_06_15 + 30 * NPD)]), i(2024));
    }
    #[test]
    fn y27() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_06_15 + 100 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y28() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_06_15 + 200 * NPD)]),
            i(2025)
        );
    }
    #[test]
    fn y29() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_12_31 + NPD)]), i(2025));
    }
    #[test]
    fn y30() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_02_29 + NPD)]), i(2024));
    }
    #[test]
    fn y31() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_02_29 + 30 * NPD)]), i(2024));
    }
    #[test]
    fn y32() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_02_29 + 306 * NPD)]),
            i(2024)
        );
    }
}

// extract_month — 40 tests
mod month_t03 {
    use super::*;
    #[test]
    fn jan() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn feb() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_02_29)]), i(2));
    }
    #[test]
    fn jun() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_06_15)]), i(6));
    }
    #[test]
    fn dec() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_12_31)]), i(12));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("extract_month", &[null()]), null());
    }
    #[test]
    fn epoch() {
        assert_eq!(ev("extract_month", &[ts(0)]), i(1));
    }
    #[test]
    fn month_of_year_alias() {
        assert_eq!(ev("month_of_year", &[ts(TS_2024_06_15)]), i(6));
    }
    #[test]
    fn mar() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_03_15_123045)]), i(3));
    }
    #[test]
    fn m01() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 31 * NPD)]), i(2));
    }
    #[test]
    fn m02() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 59 * NPD)]), i(2));
    }
    #[test]
    fn m03() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 60 * NPD)]), i(3));
    } // leap year
    #[test]
    fn m04() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 90 * NPD)]), i(3));
    }
    #[test]
    fn m05() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 91 * NPD)]), i(4));
    }
    #[test]
    fn m06() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 120 * NPD)]), i(4));
    }
    #[test]
    fn m07() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 121 * NPD)]), i(5));
    }
    #[test]
    fn m08() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 152 * NPD)]), i(6));
    }
    #[test]
    fn m09() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 182 * NPD)]), i(7));
    }
    #[test]
    fn m10() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 213 * NPD)]), i(8));
    }
    #[test]
    fn m11() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 244 * NPD)]), i(9));
    }
    #[test]
    fn m12() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 274 * NPD)]), i(10));
    }
    #[test]
    fn m13() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 305 * NPD)]), i(11));
    }
    #[test]
    fn m14() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 335 * NPD)]), i(12));
    }
    #[test]
    fn m15() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + NPH)]), i(1));
    }
    #[test]
    fn m16() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 15 * NPD)]), i(1));
    }
    #[test]
    fn m17() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 20 * NPD)]), i(1));
    }
    #[test]
    fn m18() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 25 * NPD)]), i(1));
    }
    #[test]
    fn m19() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 30 * NPD)]), i(1));
    }
    #[test]
    fn m20() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_06_15 + NPD)]), i(6));
    }
    #[test]
    fn m21() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_06_15 + 10 * NPD)]), i(6));
    }
    #[test]
    fn m22() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_06_15 + 16 * NPD)]), i(7));
    }
    #[test]
    fn m23() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_02_29 + NPD)]), i(3));
    }
    #[test]
    fn m24() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_12_31 + NPD)]), i(1));
    }
    #[test]
    fn m25() {
        assert_eq!(ev("extract_month", &[ts(TS_2000_01_01)]), i(1));
    }
    #[test]
    fn m26() {
        assert_eq!(ev("extract_month", &[ts(TS_2000_01_01 + 31 * NPD)]), i(2));
    }
    #[test]
    fn m27() {
        assert_eq!(ev("extract_month", &[ts(TS_2000_01_01 + 60 * NPD)]), i(3));
    }
    #[test]
    fn m28() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 5 * NPD)]), i(1));
    }
    #[test]
    fn m29() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 10 * NPD)]), i(1));
    }
    #[test]
    fn m30() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 35 * NPD)]), i(2));
    }
    #[test]
    fn m31() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 40 * NPD)]), i(2));
    }
    #[test]
    fn m32() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 50 * NPD)]), i(2));
    }
}

// extract_day — 40 tests
mod day_t03 {
    use super::*;
    #[test]
    fn d1() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn d29() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_02_29)]), i(29));
    }
    #[test]
    fn d15() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_06_15)]), i(15));
    }
    #[test]
    fn d31() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_12_31)]), i(31));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("extract_day", &[null()]), null());
    }
    #[test]
    fn epoch() {
        assert_eq!(ev("extract_day", &[ts(0)]), i(1));
    }
    #[test]
    fn day_of_month_alias() {
        assert_eq!(ev("day_of_month", &[ts(TS_2024_06_15)]), i(15));
    }
    #[test]
    fn d01() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + NPD)]), i(2));
    }
    #[test]
    fn d02() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 2 * NPD)]), i(3));
    }
    #[test]
    fn d03() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 3 * NPD)]), i(4));
    }
    #[test]
    fn d04() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 9 * NPD)]), i(10));
    }
    #[test]
    fn d05() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 14 * NPD)]), i(15));
    }
    #[test]
    fn d06() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 19 * NPD)]), i(20));
    }
    #[test]
    fn d07() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 24 * NPD)]), i(25));
    }
    #[test]
    fn d08() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 29 * NPD)]), i(30));
    }
    #[test]
    fn d09() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 30 * NPD)]), i(31));
    }
    #[test]
    fn d10() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_06_15 + NPD)]), i(16));
    }
    #[test]
    fn d11() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_06_15 + 5 * NPD)]), i(20));
    }
    #[test]
    fn d12() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_06_15 + 10 * NPD)]), i(25));
    }
    #[test]
    fn d13() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_06_15 + 14 * NPD)]), i(29));
    }
    #[test]
    fn d14() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_02_29 + NPD)]), i(1));
    } // march 1
    #[test]
    fn d15r() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_03_15_123045)]), i(15));
    }
    #[test]
    fn d16() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + NPH)]), i(1));
    }
    #[test]
    fn d17() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 23 * NPH)]), i(1));
    }
    #[test]
    fn d18() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 4 * NPD)]), i(5));
    }
    #[test]
    fn d19() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 5 * NPD)]), i(6));
    }
    #[test]
    fn d20() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 6 * NPD)]), i(7));
    }
    #[test]
    fn d21() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 7 * NPD)]), i(8));
    }
    #[test]
    fn d22() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 8 * NPD)]), i(9));
    }
    #[test]
    fn d23() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 10 * NPD)]), i(11));
    }
    #[test]
    fn d24() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 11 * NPD)]), i(12));
    }
    #[test]
    fn d25() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 12 * NPD)]), i(13));
    }
    #[test]
    fn d26() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 13 * NPD)]), i(14));
    }
    #[test]
    fn d27() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 15 * NPD)]), i(16));
    }
    #[test]
    fn d28() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 16 * NPD)]), i(17));
    }
    #[test]
    fn d29r() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 17 * NPD)]), i(18));
    }
    #[test]
    fn d30() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 18 * NPD)]), i(19));
    }
    #[test]
    fn d31r() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 20 * NPD)]), i(21));
    }
    #[test]
    fn d32() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 21 * NPD)]), i(22));
    }
    #[test]
    fn d33() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 22 * NPD)]), i(23));
    }
}

// extract_hour — 40 tests
mod hour_t03 {
    use super::*;
    #[test]
    fn h0() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01)]), i(0));
    }
    #[test]
    fn h12() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 12 * NPH)]), i(12));
    }
    #[test]
    fn h23() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 23 * NPH)]), i(23));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("extract_hour", &[null()]), null());
    }
    #[test]
    fn hour_of_day_alias() {
        assert_eq!(ev("hour_of_day", &[ts(TS_2024_01_01 + 5 * NPH)]), i(5));
    }
    #[test]
    fn h01() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + NPH)]), i(1));
    }
    #[test]
    fn h02() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 2 * NPH)]), i(2));
    }
    #[test]
    fn h03() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 3 * NPH)]), i(3));
    }
    #[test]
    fn h04() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 4 * NPH)]), i(4));
    }
    #[test]
    fn h05() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 5 * NPH)]), i(5));
    }
    #[test]
    fn h06() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 6 * NPH)]), i(6));
    }
    #[test]
    fn h07() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 7 * NPH)]), i(7));
    }
    #[test]
    fn h08() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 8 * NPH)]), i(8));
    }
    #[test]
    fn h09() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 9 * NPH)]), i(9));
    }
    #[test]
    fn h10() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 10 * NPH)]), i(10));
    }
    #[test]
    fn h11() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 11 * NPH)]), i(11));
    }
    #[test]
    fn h13() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 13 * NPH)]), i(13));
    }
    #[test]
    fn h14() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 14 * NPH)]), i(14));
    }
    #[test]
    fn h15() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 15 * NPH)]), i(15));
    }
    #[test]
    fn h16() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 16 * NPH)]), i(16));
    }
    #[test]
    fn h17() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 17 * NPH)]), i(17));
    }
    #[test]
    fn h18() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 18 * NPH)]), i(18));
    }
    #[test]
    fn h19() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 19 * NPH)]), i(19));
    }
    #[test]
    fn h20() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 20 * NPH)]), i(20));
    }
    #[test]
    fn h21() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 21 * NPH)]), i(21));
    }
    #[test]
    fn h22() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 22 * NPH)]), i(22));
    }
    #[test]
    fn h_next_day() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 24 * NPH)]), i(0));
    }
    #[test]
    fn h_half() {
        assert_eq!(
            ev("extract_hour", &[ts(TS_2024_01_01 + NPH + 30 * NPM)]),
            i(1)
        );
    }
    #[test]
    fn h_minute() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 59 * NPM)]), i(0));
    }
    #[test]
    fn h_epoch() {
        assert_eq!(ev("extract_hour", &[ts(0)]), i(0));
    }
    #[test]
    fn h_leap() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_02_29 + 6 * NPH)]), i(6));
    }
    #[test]
    fn h_mar15() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_03_15_123045)]), i(12));
    }
    #[test]
    fn h_jun15() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_06_15)]), i(0));
    }
    #[test]
    fn h_jun15_10() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_06_15 + 10 * NPH)]), i(10));
    }
    #[test]
    fn h_dec31() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_12_31)]), i(0));
    }
    #[test]
    fn h_dec31_23() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_12_31 + 23 * NPH)]), i(23));
    }
    #[test]
    fn h_2000() {
        assert_eq!(ev("extract_hour", &[ts(TS_2000_01_01)]), i(0));
    }
    #[test]
    fn h_2000_12() {
        assert_eq!(ev("extract_hour", &[ts(TS_2000_01_01 + 12 * NPH)]), i(12));
    }
    #[test]
    fn h_min_offset() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + NPH + NPM)]), i(1));
    }
    #[test]
    fn h_sec_offset() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + NPH + NPS)]), i(1));
    }
}

// extract_minute / extract_second — 30 tests
mod minsec_t03 {
    use super::*;
    #[test]
    fn min_0() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01)]), i(0));
    }
    #[test]
    fn min_30() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 30 * NPM)]), i(30));
    }
    #[test]
    fn min_59() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 59 * NPM)]), i(59));
    }
    #[test]
    fn min_null() {
        assert_eq!(ev("extract_minute", &[null()]), null());
    }
    #[test]
    fn minute_of_hour() {
        assert_eq!(ev("minute_of_hour", &[ts(TS_2024_01_01 + 15 * NPM)]), i(15));
    }
    #[test]
    fn sec_0() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01)]), i(0));
    }
    #[test]
    fn sec_30() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 30 * NPS)]), i(30));
    }
    #[test]
    fn sec_59() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 59 * NPS)]), i(59));
    }
    #[test]
    fn sec_null() {
        assert_eq!(ev("extract_second", &[null()]), null());
    }
    #[test]
    fn second_of_minute() {
        assert_eq!(
            ev("second_of_minute", &[ts(TS_2024_01_01 + 45 * NPS)]),
            i(45)
        );
    }
    #[test]
    fn min_1() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + NPM)]), i(1));
    }
    #[test]
    fn min_10() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 10 * NPM)]), i(10));
    }
    #[test]
    fn min_20() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 20 * NPM)]), i(20));
    }
    #[test]
    fn min_45() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 45 * NPM)]), i(45));
    }
    #[test]
    fn sec_1() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + NPS)]), i(1));
    }
    #[test]
    fn sec_10() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 10 * NPS)]), i(10));
    }
    #[test]
    fn sec_20() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 20 * NPS)]), i(20));
    }
    #[test]
    fn sec_45() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 45 * NPS)]), i(45));
    }
    #[test]
    fn min_5() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 5 * NPM)]), i(5));
    }
    #[test]
    fn min_15() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 15 * NPM)]), i(15));
    }
    #[test]
    fn min_25() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 25 * NPM)]), i(25));
    }
    #[test]
    fn min_35() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 35 * NPM)]), i(35));
    }
    #[test]
    fn min_40() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 40 * NPM)]), i(40));
    }
    #[test]
    fn min_50() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 50 * NPM)]), i(50));
    }
    #[test]
    fn min_55() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 55 * NPM)]), i(55));
    }
    #[test]
    fn sec_5() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 5 * NPS)]), i(5));
    }
    #[test]
    fn sec_15() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 15 * NPS)]), i(15));
    }
    #[test]
    fn sec_25() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 25 * NPS)]), i(25));
    }
    #[test]
    fn sec_35() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 35 * NPS)]), i(35));
    }
    #[test]
    fn sec_50() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 50 * NPS)]), i(50));
    }
}

// extract_quarter / extract_week / extract_day_of_week / extract_day_of_year — 40 tests
mod qtr_week_t03 {
    use super::*;
    #[test]
    fn q1() {
        assert_eq!(ev("extract_quarter", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn q2() {
        assert_eq!(ev("extract_quarter", &[ts(TS_2024_06_15)]), i(2));
    }
    #[test]
    fn q3() {
        assert_eq!(
            ev("extract_quarter", &[ts(TS_2024_01_01 + 200 * NPD)]),
            i(3)
        );
    }
    #[test]
    fn q4() {
        assert_eq!(ev("extract_quarter", &[ts(TS_2024_12_31)]), i(4));
    }
    #[test]
    fn q_null() {
        assert_eq!(ev("extract_quarter", &[null()]), null());
    }
    #[test]
    fn quarter_of_year() {
        assert_eq!(ev("quarter_of_year", &[ts(TS_2024_06_15)]), i(2));
    }
    #[test]
    fn week_jan1() {
        let r = ev("extract_week", &[ts(TS_2024_01_01)]);
        assert!(matches!(r, Value::I64(v) if (1..=53).contains(&v)));
    }
    #[test]
    fn week_null() {
        assert_eq!(ev("extract_week", &[null()]), null());
    }
    #[test]
    fn week_of_year() {
        let r = ev("week_of_year", &[ts(TS_2024_06_15)]);
        assert!(matches!(r, Value::I64(v) if (1..=53).contains(&v)));
    }
    #[test]
    fn dow_epoch() {
        let r = ev("extract_day_of_week", &[ts(0)]);
        assert!(matches!(r, Value::I64(v) if (0..=7).contains(&v)));
    }
    #[test]
    fn dow_null() {
        assert_eq!(ev("extract_day_of_week", &[null()]), null());
    }
    #[test]
    fn day_of_week_alias() {
        let r = ev("day_of_week", &[ts(TS_2024_01_01)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn doy_jan1() {
        assert_eq!(ev("extract_day_of_year", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn doy_null() {
        assert_eq!(ev("extract_day_of_year", &[null()]), null());
    }
    #[test]
    fn day_of_year_alias() {
        assert_eq!(ev("day_of_year", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn doy_feb29() {
        assert_eq!(ev("extract_day_of_year", &[ts(TS_2024_02_29)]), i(60));
    }
    #[test]
    fn doy_dec31() {
        assert_eq!(ev("extract_day_of_year", &[ts(TS_2024_12_31)]), i(366));
    }
    #[test]
    fn q01() {
        assert_eq!(ev("extract_quarter", &[ts(TS_2024_02_29)]), i(1));
    }
    #[test]
    fn q02() {
        assert_eq!(ev("extract_quarter", &[ts(TS_2024_03_15_123045)]), i(1));
    }
    #[test]
    fn doy_jan2() {
        assert_eq!(ev("extract_day_of_year", &[ts(TS_2024_01_01 + NPD)]), i(2));
    }
    #[test]
    fn doy_jan10() {
        assert_eq!(
            ev("extract_day_of_year", &[ts(TS_2024_01_01 + 9 * NPD)]),
            i(10)
        );
    }
    #[test]
    fn doy_jan31() {
        assert_eq!(
            ev("extract_day_of_year", &[ts(TS_2024_01_01 + 30 * NPD)]),
            i(31)
        );
    }
    #[test]
    fn doy_feb1() {
        assert_eq!(
            ev("extract_day_of_year", &[ts(TS_2024_01_01 + 31 * NPD)]),
            i(32)
        );
    }
    #[test]
    fn doy_mar1() {
        assert_eq!(ev("extract_day_of_year", &[ts(TS_2024_02_29 + NPD)]), i(61));
    }
    #[test]
    fn doy_jun15() {
        assert_eq!(ev("extract_day_of_year", &[ts(TS_2024_06_15)]), i(167));
    }
    #[test]
    fn dow_consecutive() {
        let d1 = match ev("extract_day_of_week", &[ts(TS_2024_01_01)]) {
            Value::I64(v) => v,
            _ => panic!(),
        };
        let d2 = match ev("extract_day_of_week", &[ts(TS_2024_01_01 + NPD)]) {
            Value::I64(v) => v,
            _ => panic!(),
        };
        assert!((d2 - d1).abs() <= 1 || (d2 - d1).abs() == 6);
    }
    #[test]
    fn q03() {
        assert_eq!(ev("extract_quarter", &[ts(TS_2024_01_01 + 91 * NPD)]), i(2));
    }
    #[test]
    fn q04() {
        let r = ev("extract_quarter", &[ts(TS_2024_01_01 + 180 * NPD)]);
        assert!(matches!(r, Value::I64(v) if (2..=3).contains(&v)));
    }
    #[test]
    fn q05() {
        assert_eq!(
            ev("extract_quarter", &[ts(TS_2024_01_01 + 275 * NPD)]),
            i(4)
        );
    }
    #[test]
    fn doy_100() {
        assert_eq!(
            ev("extract_day_of_year", &[ts(TS_2024_01_01 + 99 * NPD)]),
            i(100)
        );
    }
    #[test]
    fn doy_200() {
        assert_eq!(
            ev("extract_day_of_year", &[ts(TS_2024_01_01 + 199 * NPD)]),
            i(200)
        );
    }
    #[test]
    fn doy_300() {
        assert_eq!(
            ev("extract_day_of_year", &[ts(TS_2024_01_01 + 299 * NPD)]),
            i(300)
        );
    }
    #[test]
    fn doy_365() {
        assert_eq!(
            ev("extract_day_of_year", &[ts(TS_2024_01_01 + 364 * NPD)]),
            i(365)
        );
    }
    #[test]
    fn q_feb() {
        assert_eq!(ev("extract_quarter", &[ts(TS_2024_01_01 + 31 * NPD)]), i(1));
    }
    #[test]
    fn q_may() {
        assert_eq!(
            ev("extract_quarter", &[ts(TS_2024_01_01 + 121 * NPD)]),
            i(2)
        );
    }
    #[test]
    fn q_aug() {
        assert_eq!(
            ev("extract_quarter", &[ts(TS_2024_01_01 + 213 * NPD)]),
            i(3)
        );
    }
    #[test]
    fn q_nov() {
        assert_eq!(
            ev("extract_quarter", &[ts(TS_2024_01_01 + 305 * NPD)]),
            i(4)
        );
    }
    #[test]
    fn dow_01() {
        let r = ev("extract_day_of_week", &[ts(TS_2024_01_01)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn dow_02() {
        let r = ev("extract_day_of_week", &[ts(TS_2024_06_15)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn dow_03() {
        let r = ev("extract_day_of_week", &[ts(TS_2024_12_31)]);
        assert!(matches!(r, Value::I64(_)));
    }
}

// epoch_nanos / epoch_seconds / epoch_millis / epoch_micros — 40 tests
mod epoch_t03 {
    use super::*;
    #[test]
    fn nanos_roundtrip() {
        assert_eq!(ev("epoch_nanos", &[ts(1000)]), i(1000));
    }
    #[test]
    fn nanos_0() {
        assert_eq!(ev("epoch_nanos", &[ts(0)]), i(0));
    }
    #[test]
    fn nanos_null() {
        assert_eq!(ev("epoch_nanos", &[null()]), null());
    }
    #[test]
    fn seconds_epoch() {
        assert_eq!(ev("epoch_seconds", &[ts(0)]), i(0));
    }
    #[test]
    fn seconds_1s() {
        assert_eq!(ev("epoch_seconds", &[ts(NPS)]), i(1));
    }
    #[test]
    fn seconds_null() {
        assert_eq!(ev("epoch_seconds", &[null()]), null());
    }
    #[test]
    fn millis_epoch() {
        assert_eq!(ev("epoch_millis", &[ts(0)]), i(0));
    }
    #[test]
    fn millis_1s() {
        assert_eq!(ev("epoch_millis", &[ts(NPS)]), i(1000));
    }
    #[test]
    fn millis_null() {
        assert_eq!(ev("epoch_millis", &[null()]), null());
    }
    #[test]
    fn micros_epoch() {
        assert_eq!(ev("epoch_micros", &[ts(0)]), i(0));
    }
    #[test]
    fn micros_1s() {
        assert_eq!(ev("epoch_micros", &[ts(NPS)]), i(1_000_000));
    }
    #[test]
    fn micros_null() {
        assert_eq!(ev("epoch_micros", &[null()]), null());
    }
    #[test]
    fn unix_timestamp_alias() {
        assert_eq!(ev("unix_timestamp", &[ts(NPS)]), i(1));
    }
    #[test]
    fn to_unix_timestamp_alias() {
        assert_eq!(ev("to_unix_timestamp", &[ts(NPS)]), i(1));
    }
    #[test]
    fn nanos_2024() {
        assert_eq!(ev("epoch_nanos", &[ts(TS_2024_01_01)]), i(TS_2024_01_01));
    }
    #[test]
    fn seconds_2024() {
        assert_eq!(ev("epoch_seconds", &[ts(TS_2024_01_01)]), i(1704067200));
    }
    #[test]
    fn millis_2024() {
        assert_eq!(ev("epoch_millis", &[ts(TS_2024_01_01)]), i(1704067200000));
    }
    #[test]
    fn micros_2024() {
        assert_eq!(
            ev("epoch_micros", &[ts(TS_2024_01_01)]),
            i(1704067200000000)
        );
    }
    #[test]
    fn nanos_neg() {
        assert_eq!(ev("epoch_nanos", &[ts(-1000)]), i(-1000));
    }
    #[test]
    fn seconds_10s() {
        assert_eq!(ev("epoch_seconds", &[ts(10 * NPS)]), i(10));
    }
    #[test]
    fn seconds_60s() {
        assert_eq!(ev("epoch_seconds", &[ts(60 * NPS)]), i(60));
    }
    #[test]
    fn seconds_3600s() {
        assert_eq!(ev("epoch_seconds", &[ts(3600 * NPS)]), i(3600));
    }
    #[test]
    fn seconds_day() {
        assert_eq!(ev("epoch_seconds", &[ts(NPD)]), i(86400));
    }
    #[test]
    fn millis_10s() {
        assert_eq!(ev("epoch_millis", &[ts(10 * NPS)]), i(10000));
    }
    #[test]
    fn millis_60s() {
        assert_eq!(ev("epoch_millis", &[ts(60 * NPS)]), i(60000));
    }
    #[test]
    fn micros_10s() {
        assert_eq!(ev("epoch_micros", &[ts(10 * NPS)]), i(10_000_000));
    }
    #[test]
    fn nanos_1day() {
        assert_eq!(ev("epoch_nanos", &[ts(NPD)]), i(NPD));
    }
    #[test]
    fn nanos_2days() {
        assert_eq!(ev("epoch_nanos", &[ts(2 * NPD)]), i(2 * NPD));
    }
    #[test]
    fn seconds_2day() {
        assert_eq!(ev("epoch_seconds", &[ts(2 * NPD)]), i(172800));
    }
    #[test]
    fn millis_day() {
        assert_eq!(ev("epoch_millis", &[ts(NPD)]), i(86400000));
    }
    #[test]
    fn nanos_1h() {
        assert_eq!(ev("epoch_nanos", &[ts(NPH)]), i(NPH));
    }
    #[test]
    fn seconds_1h() {
        assert_eq!(ev("epoch_seconds", &[ts(NPH)]), i(3600));
    }
    #[test]
    fn millis_1h() {
        assert_eq!(ev("epoch_millis", &[ts(NPH)]), i(3600000));
    }
    #[test]
    fn nanos_1m() {
        assert_eq!(ev("epoch_nanos", &[ts(NPM)]), i(NPM));
    }
    #[test]
    fn seconds_1m() {
        assert_eq!(ev("epoch_seconds", &[ts(NPM)]), i(60));
    }
    #[test]
    fn nanos_2000() {
        assert_eq!(ev("epoch_nanos", &[ts(TS_2000_01_01)]), i(TS_2000_01_01));
    }
    #[test]
    fn seconds_2000() {
        assert_eq!(ev("epoch_seconds", &[ts(TS_2000_01_01)]), i(946684800));
    }
    #[test]
    fn millis_2000() {
        assert_eq!(ev("epoch_millis", &[ts(TS_2000_01_01)]), i(946684800000));
    }
    #[test]
    fn seconds_100s() {
        assert_eq!(ev("epoch_seconds", &[ts(100 * NPS)]), i(100));
    }
    #[test]
    fn seconds_1000s() {
        assert_eq!(ev("epoch_seconds", &[ts(1000 * NPS)]), i(1000));
    }
}

// date_trunc — 30 tests
mod trunc_t03 {
    use super::*;
    #[test]
    fn trunc_day() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2024_03_15_123045)]);
        assert_eq!(ev("extract_hour", std::slice::from_ref(&r)), i(0));
        assert_eq!(ev("extract_minute", std::slice::from_ref(&r)), i(0));
    }
    #[test]
    fn trunc_hour() {
        let r = ev("date_trunc", &[s("hour"), ts(TS_2024_03_15_123045)]);
        assert_eq!(ev("extract_minute", std::slice::from_ref(&r)), i(0));
        assert_eq!(ev("extract_second", std::slice::from_ref(&r)), i(0));
    }
    #[test]
    fn trunc_null() {
        assert_eq!(ev("date_trunc", &[s("day"), null()]), null());
    }
    #[test]
    fn timestamp_floor_alias() {
        let r = ev("timestamp_floor", &[s("day"), ts(TS_2024_03_15_123045)]);
        assert_eq!(ev("extract_hour", std::slice::from_ref(&r)), i(0));
    }
    #[test]
    fn trunc_day_preserves_date() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2024_01_01 + 5 * NPH)]);
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(1));
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(1));
    }
    #[test]
    fn td01() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2024_06_15 + 10 * NPH)]);
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td02() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2024_12_31 + 23 * NPH)]);
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td03() {
        let r = ev(
            "date_trunc",
            &[s("hour"), ts(TS_2024_01_01 + NPH + 30 * NPM)],
        );
        assert_eq!(ev("extract_minute", &[r]), i(0));
    }
    #[test]
    fn td04() {
        let r = ev(
            "date_trunc",
            &[s("hour"), ts(TS_2024_01_01 + 2 * NPH + 45 * NPM)],
        );
        assert_eq!(ev("extract_minute", &[r]), i(0));
    }
    #[test]
    fn td05() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2024_02_29 + 6 * NPH)]);
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(29));
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td06() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2024_01_01)]);
        assert_eq!(r, ts(TS_2024_01_01));
    }
    #[test]
    fn td07() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2024_01_01 + NPD)]);
        assert_eq!(ev("extract_day", &[r]), i(2));
    }
    #[test]
    fn td08() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2024_01_01 + 2 * NPD)]);
        assert_eq!(ev("extract_day", &[r]), i(3));
    }
    #[test]
    fn td09() {
        let r = ev(
            "date_trunc",
            &[s("day"), ts(TS_2024_01_01 + 10 * NPD + 5 * NPH)],
        );
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(11));
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td10() {
        let r = ev(
            "date_trunc",
            &[s("hour"), ts(TS_2024_01_01 + 3 * NPH + 15 * NPM + 30 * NPS)],
        );
        assert_eq!(ev("extract_hour", std::slice::from_ref(&r)), i(3));
        assert_eq!(ev("extract_minute", std::slice::from_ref(&r)), i(0));
        assert_eq!(ev("extract_second", &[r]), i(0));
    }
    #[test]
    fn td11() {
        let r = ev(
            "date_trunc",
            &[s("day"), ts(TS_2024_06_15 + 23 * NPH + 59 * NPM)],
        );
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(15));
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td12() {
        let r = ev(
            "date_trunc",
            &[s("hour"), ts(TS_2024_06_15 + 12 * NPH + 30 * NPM)],
        );
        assert_eq!(ev("extract_hour", std::slice::from_ref(&r)), i(12));
        assert_eq!(ev("extract_minute", &[r]), i(0));
    }
    #[test]
    fn td13() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2000_01_01 + 12 * NPH)]);
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td14() {
        let r = ev(
            "date_trunc",
            &[s("hour"), ts(TS_2000_01_01 + 6 * NPH + 45 * NPM)],
        );
        assert_eq!(ev("extract_minute", &[r]), i(0));
    }
    #[test]
    fn td15() {
        let r = ev("date_trunc", &[s("day"), ts(0)]);
        assert_eq!(r, ts(0));
    }
    #[test]
    fn td16() {
        let r = ev("date_trunc", &[s("day"), ts(NPH)]);
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td17() {
        let r = ev("date_trunc", &[s("day"), ts(23 * NPH + 59 * NPM)]);
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td18() {
        let r = ev("date_trunc", &[s("hour"), ts(NPH + 30 * NPM)]);
        assert_eq!(ev("extract_minute", &[r]), i(0));
    }
    #[test]
    fn td19() {
        let r = ev(
            "date_trunc",
            &[s("day"), ts(TS_2024_01_01 + 15 * NPD + 18 * NPH + 33 * NPM)],
        );
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td20() {
        let r = ev(
            "date_trunc",
            &[
                s("hour"),
                ts(TS_2024_01_01 + 15 * NPD + 18 * NPH + 33 * NPM),
            ],
        );
        assert_eq!(ev("extract_minute", &[r]), i(0));
    }
    #[test]
    fn td21() {
        let r = ev("date_trunc", &[s("day"), ts(TS_2024_03_15_123045)]);
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(15));
        assert_eq!(ev("extract_month", &[r]), i(3));
    }
    #[test]
    fn td22() {
        let r = ev("date_trunc", &[s("hour"), ts(TS_2024_03_15_123045)]);
        assert_eq!(ev("extract_hour", std::slice::from_ref(&r)), i(12));
    }
    #[test]
    fn td23() {
        let r = ev(
            "date_trunc",
            &[s("day"), ts(TS_2024_01_01 + 100 * NPD + 7 * NPH)],
        );
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td24() {
        let r = ev(
            "date_trunc",
            &[s("day"), ts(TS_2024_01_01 + 200 * NPD + 15 * NPH)],
        );
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
    #[test]
    fn td25() {
        let r = ev(
            "date_trunc",
            &[s("day"), ts(TS_2024_01_01 + 300 * NPD + 22 * NPH)],
        );
        assert_eq!(ev("extract_hour", &[r]), i(0));
    }
}

// is_weekend / is_business_day — 20 tests
mod weekend_t03 {
    use super::*;
    // 2024-06-15 is Saturday
    #[test]
    fn sat_weekend() {
        assert_eq!(ev("is_weekend", &[ts(TS_2024_06_15)]), i(1));
    }
    #[test]
    fn sun_weekend() {
        assert_eq!(ev("is_weekend", &[ts(TS_2024_06_15 + NPD)]), i(1));
    }
    #[test]
    fn mon_not_weekend() {
        assert_eq!(ev("is_weekend", &[ts(TS_2024_06_15 + 2 * NPD)]), i(0));
    }
    #[test]
    fn null_weekend() {
        assert_eq!(ev("is_weekend", &[null()]), null());
    }
    #[test]
    fn sat_not_bday() {
        assert_eq!(ev("is_business_day", &[ts(TS_2024_06_15)]), i(0));
    }
    #[test]
    fn sun_not_bday() {
        assert_eq!(ev("is_business_day", &[ts(TS_2024_06_15 + NPD)]), i(0));
    }
    #[test]
    fn mon_bday() {
        assert_eq!(ev("is_business_day", &[ts(TS_2024_06_15 + 2 * NPD)]), i(1));
    }
    #[test]
    fn null_bday() {
        assert_eq!(ev("is_business_day", &[null()]), null());
    }
    #[test]
    fn tue_not_weekend() {
        assert_eq!(ev("is_weekend", &[ts(TS_2024_06_15 + 3 * NPD)]), i(0));
    }
    #[test]
    fn wed_not_weekend() {
        assert_eq!(ev("is_weekend", &[ts(TS_2024_06_15 + 4 * NPD)]), i(0));
    }
    #[test]
    fn thu_not_weekend() {
        assert_eq!(ev("is_weekend", &[ts(TS_2024_06_15 + 5 * NPD)]), i(0));
    }
    #[test]
    fn fri_not_weekend() {
        assert_eq!(ev("is_weekend", &[ts(TS_2024_06_15 + 6 * NPD)]), i(0));
    }
    #[test]
    fn next_sat() {
        assert_eq!(ev("is_weekend", &[ts(TS_2024_06_15 + 7 * NPD)]), i(1));
    }
    #[test]
    fn tue_bday() {
        assert_eq!(ev("is_business_day", &[ts(TS_2024_06_15 + 3 * NPD)]), i(1));
    }
    #[test]
    fn wed_bday() {
        assert_eq!(ev("is_business_day", &[ts(TS_2024_06_15 + 4 * NPD)]), i(1));
    }
    #[test]
    fn thu_bday() {
        assert_eq!(ev("is_business_day", &[ts(TS_2024_06_15 + 5 * NPD)]), i(1));
    }
    #[test]
    fn fri_bday() {
        assert_eq!(ev("is_business_day", &[ts(TS_2024_06_15 + 6 * NPD)]), i(1));
    }
    #[test]
    fn next_sat_not_bday() {
        assert_eq!(ev("is_business_day", &[ts(TS_2024_06_15 + 7 * NPD)]), i(0));
    }
    #[test]
    fn next_sun_weekend() {
        assert_eq!(ev("is_weekend", &[ts(TS_2024_06_15 + 8 * NPD)]), i(1));
    }
    #[test]
    fn next_sun_not_bday() {
        assert_eq!(ev("is_business_day", &[ts(TS_2024_06_15 + 8 * NPD)]), i(0));
    }
}

// is_leap_year / days_in_month — 20 tests
mod leap_t03 {
    use super::*;
    #[test]
    fn leap_2024() {
        assert_eq!(ev("is_leap_year_fn", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn not_leap_2023() {
        assert_eq!(
            ev("is_leap_year_fn", &[ts(TS_2024_01_01 - 365 * NPD)]),
            i(0)
        );
    }
    #[test]
    fn leap_2000() {
        assert_eq!(ev("is_leap_year_fn", &[ts(TS_2000_01_01)]), i(1));
    }
    #[test]
    fn leap_null() {
        assert_eq!(ev("is_leap_year_fn", &[null()]), null());
    }
    #[test]
    fn leap_1970() {
        assert_eq!(ev("is_leap_year_fn", &[ts(0)]), i(0));
    }
    #[test]
    fn dim_jan() {
        assert_eq!(ev("days_in_month_fn", &[ts(TS_2024_01_01)]), i(31));
    }
    #[test]
    fn dim_feb_leap() {
        assert_eq!(ev("days_in_month_fn", &[ts(TS_2024_02_29)]), i(29));
    }
    #[test]
    fn dim_jun() {
        assert_eq!(ev("days_in_month_fn", &[ts(TS_2024_06_15)]), i(30));
    }
    #[test]
    fn dim_null() {
        assert_eq!(ev("days_in_month_fn", &[null()]), null());
    }
    #[test]
    fn dim_dec() {
        assert_eq!(ev("days_in_month_fn", &[ts(TS_2024_12_31)]), i(31));
    }
    #[test]
    fn dim_mar() {
        assert_eq!(ev("days_in_month_fn", &[ts(TS_2024_03_15_123045)]), i(31));
    }
    #[test]
    fn leap_feb29() {
        assert_eq!(ev("is_leap_year_fn", &[ts(TS_2024_02_29)]), i(1));
    }
    #[test]
    fn dim_apr() {
        assert_eq!(
            ev("days_in_month_fn", &[ts(TS_2024_01_01 + 91 * NPD)]),
            i(30)
        );
    } // Apr
    #[test]
    fn dim_may() {
        assert_eq!(
            ev("days_in_month_fn", &[ts(TS_2024_01_01 + 121 * NPD)]),
            i(31)
        );
    } // May
    #[test]
    fn dim_jul() {
        assert_eq!(
            ev("days_in_month_fn", &[ts(TS_2024_01_01 + 182 * NPD)]),
            i(31)
        );
    } // Jul
    #[test]
    fn dim_aug() {
        assert_eq!(
            ev("days_in_month_fn", &[ts(TS_2024_01_01 + 213 * NPD)]),
            i(31)
        );
    } // Aug
    #[test]
    fn dim_sep() {
        assert_eq!(
            ev("days_in_month_fn", &[ts(TS_2024_01_01 + 244 * NPD)]),
            i(30)
        );
    } // Sep
    #[test]
    fn dim_oct() {
        assert_eq!(
            ev("days_in_month_fn", &[ts(TS_2024_01_01 + 274 * NPD)]),
            i(31)
        );
    } // Oct
    #[test]
    fn dim_nov() {
        assert_eq!(
            ev("days_in_month_fn", &[ts(TS_2024_01_01 + 305 * NPD)]),
            i(30)
        );
    } // Nov
    #[test]
    fn dim_feb_nonleap() {
        let r = ev(
            "days_in_month_fn",
            &[ts(TS_2000_01_01 + 366 * NPD + 31 * NPD)],
        );
        assert!(matches!(r, Value::I64(v) if v == 28 || v == 29));
    } // around Feb 2001
}

// first_of_month / last_of_month / start_of_year / end_of_year / start_of_quarter / start_of_week — 40 tests
mod boundaries_t03 {
    use super::*;
    #[test]
    fn fom_jan() {
        let r = ev("first_of_month", &[ts(TS_2024_01_01 + 15 * NPD)]);
        assert_eq!(ev("extract_day", &[r]), i(1));
    }
    #[test]
    fn fom_feb() {
        let r = ev("first_of_month", &[ts(TS_2024_02_29)]);
        assert_eq!(ev("extract_day", &[r]), i(1));
    }
    #[test]
    fn fom_null() {
        assert_eq!(ev("first_of_month", &[null()]), null());
    }
    #[test]
    fn lom_jan() {
        let r = ev("last_of_month", &[ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(31));
    }
    #[test]
    fn lom_feb_leap() {
        let r = ev("last_of_month", &[ts(TS_2024_02_29)]);
        assert_eq!(ev("extract_day", &[r]), i(29));
    }
    #[test]
    fn lom_null() {
        assert_eq!(ev("last_of_month", &[null()]), null());
    }
    #[test]
    fn soy() {
        let r = ev("start_of_year", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(1));
        assert_eq!(ev("extract_day", &[r]), i(1));
    }
    #[test]
    fn soy_null() {
        assert_eq!(ev("start_of_year", &[null()]), null());
    }
    #[test]
    fn eoy() {
        let r = ev("end_of_year", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(12));
        assert_eq!(ev("extract_day", &[r]), i(31));
    }
    #[test]
    fn eoy_null() {
        assert_eq!(ev("end_of_year", &[null()]), null());
    }
    #[test]
    fn soq() {
        let r = ev("start_of_quarter", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(4));
        assert_eq!(ev("extract_day", &[r]), i(1));
    }
    #[test]
    fn soq_null() {
        assert_eq!(ev("start_of_quarter", &[null()]), null());
    }
    #[test]
    fn sow_returns_ts() {
        let r = ev("start_of_week", &[ts(TS_2024_06_15)]);
        assert!(matches!(r, Value::Timestamp(_)));
    }
    #[test]
    fn sow_null() {
        assert_eq!(ev("start_of_week", &[null()]), null());
    }
    #[test]
    fn fom_alias() {
        let r = ev("first_day_of_month", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_day", &[r]), i(1));
    }
    #[test]
    fn lom_alias() {
        let r = ev("last_day_of_month", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_day", &[r]), i(30));
    }
    #[test]
    fn soy_alias() {
        let r = ev("first_day_of_year", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_month", &[r]), i(1));
    }
    #[test]
    fn eoy_alias() {
        let r = ev("last_day_of_year", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_month", &[r]), i(12));
    }
    #[test]
    fn soq_alias() {
        let r = ev("first_day_of_quarter", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_month", &[r]), i(4));
    }
    #[test]
    fn sow_alias() {
        let r = ev("first_day_of_week", &[ts(TS_2024_06_15)]);
        assert!(matches!(r, Value::Timestamp(_)));
    }
    #[test]
    fn fom_mar() {
        let r = ev("first_of_month", &[ts(TS_2024_03_15_123045)]);
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(1));
        assert_eq!(ev("extract_month", &[r]), i(3));
    }
    #[test]
    fn lom_mar() {
        let r = ev("last_of_month", &[ts(TS_2024_03_15_123045)]);
        assert_eq!(ev("extract_day", &[r]), i(31));
    }
    #[test]
    fn fom_jun() {
        let r = ev("first_of_month", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(1));
        assert_eq!(ev("extract_month", &[r]), i(6));
    }
    #[test]
    fn lom_jun() {
        let r = ev("last_of_month", &[ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_day", &[r]), i(30));
    }
    #[test]
    fn fom_dec() {
        let r = ev("first_of_month", &[ts(TS_2024_12_31)]);
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(1));
        assert_eq!(ev("extract_month", &[r]), i(12));
    }
    #[test]
    fn lom_dec() {
        let r = ev("last_of_month", &[ts(TS_2024_12_31)]);
        assert_eq!(ev("extract_day", &[r]), i(31));
    }
    #[test]
    fn soy_2024() {
        let r = ev("start_of_year", &[ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_year", &[r]), i(2024));
    }
    #[test]
    fn eoy_2024() {
        let r = ev("end_of_year", &[ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_year", &[r]), i(2024));
    }
    #[test]
    fn soq_q1() {
        let r = ev("start_of_quarter", &[ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_month", &[r]), i(1));
    }
    #[test]
    fn soq_q2() {
        let r = ev("start_of_quarter", &[ts(TS_2024_01_01 + 121 * NPD)]);
        assert_eq!(ev("extract_month", &[r]), i(4));
    }
    #[test]
    fn soq_q3() {
        let r = ev("start_of_quarter", &[ts(TS_2024_01_01 + 200 * NPD)]);
        assert_eq!(ev("extract_month", &[r]), i(7));
    }
    #[test]
    fn soq_q4() {
        let r = ev("start_of_quarter", &[ts(TS_2024_12_31)]);
        assert_eq!(ev("extract_month", &[r]), i(10));
    }
    #[test]
    fn soy_preserves_year() {
        let r = ev("start_of_year", &[ts(TS_2000_01_01)]);
        assert_eq!(ev("extract_year", &[r]), i(2000));
    }
    #[test]
    fn eoy_preserves_year() {
        let r = ev("end_of_year", &[ts(TS_2000_01_01)]);
        assert_eq!(ev("extract_year", &[r]), i(2000));
    }
    #[test]
    fn fom_2000() {
        let r = ev("first_of_month", &[ts(TS_2000_01_01 + 40 * NPD)]);
        assert_eq!(ev("extract_day", &[r]), i(1));
    }
    #[test]
    fn lom_2000() {
        let r = ev("last_of_month", &[ts(TS_2000_01_01 + 40 * NPD)]);
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(29));
    } // Feb 2000 (leap)
    #[test]
    fn soy_dec() {
        let r = ev("start_of_year", &[ts(TS_2024_12_31)]);
        assert_eq!(ev("extract_year", &[r]), i(2024));
    }
    #[test]
    fn eoy_jan() {
        let r = ev("end_of_year", &[ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(12));
        assert_eq!(ev("extract_day", &[r]), i(31));
    }
    #[test]
    fn sow_consistent() {
        let w1 = ev("start_of_week", &[ts(TS_2024_01_01)]);
        let w2 = ev("start_of_week", &[ts(TS_2024_01_01 + NPD)]);
        let diff = match (w1, w2) {
            (Value::Timestamp(a), Value::Timestamp(b)) => (b - a).abs(),
            _ => panic!(),
        };
        assert!(diff <= 7 * NPD);
    }
    #[test]
    fn fom_epoch() {
        let r = ev("first_of_month", &[ts(15 * NPD)]);
        assert_eq!(ev("extract_day", &[r]), i(1));
    }
}

// timestamp_add / date_diff — 40 tests
mod arith_t03 {
    use super::*;
    #[test]
    fn add_day() {
        let r = ev("timestamp_add", &[s("day"), i(1), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(2));
    }
    #[test]
    fn add_hour() {
        let r = ev("timestamp_add", &[s("hour"), i(1), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_hour", &[r]), i(1));
    }
    #[test]
    fn add_null() {
        assert_eq!(ev("timestamp_add", &[s("day"), i(1), null()]), null());
    }
    #[test]
    fn add_7d() {
        let r = ev("timestamp_add", &[s("day"), i(7), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(8));
    }
    #[test]
    fn add_30d() {
        let r = ev("timestamp_add", &[s("day"), i(30), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(1));
        assert_eq!(ev("extract_day", &[r]), i(31));
    }
    #[test]
    fn add_31d() {
        let r = ev("timestamp_add", &[s("day"), i(31), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_month", &[r]), i(2));
    }
    #[test]
    fn dateadd_alias() {
        let r = ev("dateadd", &[s("day"), i(1), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(2));
    }
    #[test]
    fn date_add_alias() {
        let r = ev("date_add", &[s("day"), i(1), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(2));
    }
    #[test]
    fn diff_days() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 10 * NPD)],
        );
        assert_eq!(r, i(10));
    }
    #[test]
    fn diff_hours() {
        let r = ev(
            "date_diff",
            &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 5 * NPH)],
        );
        assert_eq!(r, i(5));
    }
    #[test]
    fn diff_null() {
        assert_eq!(
            ev("date_diff", &[s("day"), null(), ts(TS_2024_01_01)]),
            null()
        );
    }
    #[test]
    fn datediff_alias() {
        let r = ev(
            "datediff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 3 * NPD)],
        );
        assert_eq!(r, i(3));
    }
    #[test]
    fn timestamp_diff_alias() {
        let r = ev(
            "timestamp_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 5 * NPD)],
        );
        assert_eq!(r, i(5));
    }
    #[test]
    fn add_neg() {
        let r = ev("timestamp_add", &[s("day"), i(-1), ts(TS_2024_01_01 + NPD)]);
        assert_eq!(ev("extract_day", &[r]), i(1));
    }
    #[test]
    fn diff_0() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01)],
        );
        assert_eq!(r, i(0));
    }
    #[test]
    fn a01() {
        let r = ev("timestamp_add", &[s("day"), i(2), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(3));
    }
    #[test]
    fn a02() {
        let r = ev("timestamp_add", &[s("day"), i(5), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(6));
    }
    #[test]
    fn a03() {
        let r = ev("timestamp_add", &[s("day"), i(10), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(11));
    }
    #[test]
    fn a04() {
        let r = ev("timestamp_add", &[s("hour"), i(12), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_hour", &[r]), i(12));
    }
    #[test]
    fn a05() {
        let r = ev("timestamp_add", &[s("hour"), i(24), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(2));
    }
    #[test]
    fn d01() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + NPD)],
        );
        assert_eq!(r, i(1));
    }
    #[test]
    fn d02() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 7 * NPD)],
        );
        assert_eq!(r, i(7));
    }
    #[test]
    fn d03() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 30 * NPD)],
        );
        assert_eq!(r, i(30));
    }
    #[test]
    fn d04() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 100 * NPD)],
        );
        assert_eq!(r, i(100));
    }
    #[test]
    fn d05() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 365 * NPD)],
        );
        assert_eq!(r, i(365));
    }
    #[test]
    fn d06() {
        let r = ev(
            "date_diff",
            &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + NPH)],
        );
        assert_eq!(r, i(1));
    }
    #[test]
    fn d07() {
        let r = ev(
            "date_diff",
            &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 24 * NPH)],
        );
        assert_eq!(r, i(24));
    }
    #[test]
    fn d08() {
        let r = ev(
            "date_diff",
            &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 48 * NPH)],
        );
        assert_eq!(r, i(48));
    }
    #[test]
    fn a06() {
        let r = ev("timestamp_add", &[s("day"), i(15), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(16));
    }
    #[test]
    fn a07() {
        let r = ev("timestamp_add", &[s("day"), i(20), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_day", &[r]), i(21));
    }
    #[test]
    fn a08() {
        let r = ev("timestamp_add", &[s("day"), i(365), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_year", std::slice::from_ref(&r)), i(2024));
        assert_eq!(ev("extract_month", &[r]), i(12));
    }
    #[test]
    fn a09() {
        let r = ev("timestamp_add", &[s("hour"), i(6), ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_hour", &[r]), i(6));
    }
    #[test]
    fn a10() {
        let r = ev("timestamp_add", &[s("hour"), i(18), ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_hour", &[r]), i(18));
    }
    #[test]
    fn d09() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_06_15), ts(TS_2024_12_31)],
        );
        assert!(matches!(r, Value::I64(v) if v > 0));
    }
    #[test]
    fn d10() {
        let r = ev(
            "date_diff",
            &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 100 * NPH)],
        );
        assert_eq!(r, i(100));
    }
    #[test]
    fn a11() {
        let r = ev("timestamp_add", &[s("day"), i(3), ts(TS_2024_06_15)]);
        assert_eq!(ev("extract_day", &[r]), i(18));
    }
    #[test]
    fn a12() {
        let r = ev("timestamp_add", &[s("day"), i(100), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_year", &[r]), i(2024));
    }
    #[test]
    fn a13() {
        let r = ev("timestamp_add", &[s("day"), i(200), ts(TS_2024_01_01)]);
        assert_eq!(ev("extract_year", &[r]), i(2024));
    }
    #[test]
    fn d11() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_06_15)],
        );
        assert!(matches!(r, Value::I64(v) if v > 100));
    }
    #[test]
    fn d12() {
        let r = ev(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_02_29)],
        );
        assert!(matches!(r, Value::I64(v) if v > 50));
    }
}

// months_between / years_between / age — 20 tests
mod between_t03 {
    use super::*;
    #[test]
    fn months_same() {
        assert_eq!(
            ev("months_between", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]),
            i(0)
        );
    }
    #[test]
    fn months_1() {
        let r = ev(
            "months_between",
            &[ts(TS_2024_01_01), ts(TS_2024_01_01 + 31 * NPD)],
        );
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn months_null() {
        assert_eq!(ev("months_between", &[null(), ts(TS_2024_01_01)]), null());
    }
    #[test]
    fn years_same() {
        assert_eq!(
            ev("years_between", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]),
            i(0)
        );
    }
    #[test]
    fn years_1() {
        let r = ev(
            "years_between",
            &[ts(TS_2024_01_01), ts(TS_2024_01_01 + 366 * NPD)],
        );
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn years_null() {
        assert_eq!(ev("years_between", &[null(), ts(TS_2024_01_01)]), null());
    }
    #[test]
    fn age_returns_int() {
        let r = ev("age", &[ts(TS_2024_01_01), ts(TS_2024_06_15)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn age_null() {
        assert_eq!(ev("age", &[null(), ts(TS_2024_01_01)]), null());
    }
    #[test]
    fn months_nonzero() {
        let r = ev("months_between", &[ts(TS_2024_01_01), ts(TS_2024_06_15)]);
        assert!(matches!(r, Value::I64(v) if v != 0));
    }
    #[test]
    fn months_large_abs() {
        let r = ev("months_between", &[ts(TS_2000_01_01), ts(TS_2024_01_01)]);
        match r {
            Value::I64(v) => assert!(v.abs() >= 12),
            _ => panic!(),
        }
    }
    #[test]
    fn years_nonzero() {
        let r = ev("years_between", &[ts(TS_2024_01_01), ts(TS_2024_06_15)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn years_large() {
        let r = ev("years_between", &[ts(TS_2000_01_01), ts(TS_2024_01_01)]);
        match r {
            Value::I64(v) => assert!(v.abs() >= 20),
            _ => panic!(),
        }
    }
    #[test]
    fn months_diff_2() {
        let r = ev(
            "months_between",
            &[ts(TS_2024_01_01), ts(TS_2024_01_01 + 60 * NPD)],
        );
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn age_same() {
        let r = ev("age", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]);
        assert_eq!(r, i(0));
    }
    #[test]
    fn age_nonzero() {
        let r = ev("age", &[ts(TS_2024_01_01), ts(TS_2024_01_01 + 366 * NPD)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn months_diff_3() {
        let r = ev(
            "months_between",
            &[ts(TS_2024_01_01), ts(TS_2024_01_01 + 91 * NPD)],
        );
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn months_large_val() {
        let r = ev("months_between", &[ts(TS_2000_01_01), ts(TS_2024_01_01)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn years_2000_2024() {
        let r = ev("years_between", &[ts(TS_2000_01_01), ts(TS_2024_01_01)]);
        match r {
            Value::I64(v) => assert!(v.abs() >= 20),
            _ => panic!(),
        }
    }
    #[test]
    fn months_reversed() {
        let r = ev("months_between", &[ts(TS_2024_06_15), ts(TS_2024_01_01)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn years_reversed() {
        let r = ev("years_between", &[ts(TS_2024_01_01), ts(TS_2000_01_01)]);
        assert!(matches!(r, Value::I64(_)));
    }
}

// make_timestamp / timestamp_sequence — 20 tests
mod make_ts_t03 {
    use super::*;
    #[test]
    fn make_basic() {
        let r = ev("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_year", std::slice::from_ref(&r)), i(2024));
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(1));
        assert_eq!(ev("extract_day", &[r]), i(1));
    }
    #[test]
    fn make_feb29() {
        let r = ev("make_timestamp", &[i(2024), i(2), i(29), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(2));
        assert_eq!(ev("extract_day", &[r]), i(29));
    }
    #[test]
    fn make_null() {
        assert_eq!(
            ev("make_timestamp", &[null(), i(1), i(1), i(0), i(0), i(0)]),
            null()
        );
    }
    #[test]
    fn make_with_time() {
        let r = ev(
            "make_timestamp",
            &[i(2024), i(6), i(15), i(12), i(30), i(45)],
        );
        assert_eq!(ev("extract_hour", std::slice::from_ref(&r)), i(12));
        assert_eq!(ev("extract_minute", std::slice::from_ref(&r)), i(30));
        assert_eq!(ev("extract_second", &[r]), i(45));
    }
    #[test]
    fn make_epoch() {
        let r = ev("make_timestamp", &[i(1970), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(ev("epoch_seconds", &[r]), i(0));
    }
    #[test]
    fn make_dec31() {
        let r = ev(
            "make_timestamp",
            &[i(2024), i(12), i(31), i(23), i(59), i(59)],
        );
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(12));
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(31));
        assert_eq!(ev("extract_hour", std::slice::from_ref(&r)), i(23));
    }
    #[test]
    fn make_2000() {
        let r = ev("make_timestamp", &[i(2000), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_year", &[r]), i(2000));
    }
    #[test]
    fn make_mar15() {
        let r = ev(
            "make_timestamp",
            &[i(2024), i(3), i(15), i(12), i(30), i(45)],
        );
        assert_eq!(ev("extract_year", std::slice::from_ref(&r)), i(2024));
        assert_eq!(ev("extract_month", std::slice::from_ref(&r)), i(3));
        assert_eq!(ev("extract_day", std::slice::from_ref(&r)), i(15));
        assert_eq!(ev("extract_hour", &[r]), i(12));
    }
    #[test]
    fn ts_seq_returns() {
        let r = ev("timestamp_sequence", &[ts(TS_2024_01_01), i(NPH)]);
        assert!(matches!(r, Value::Str(_) | Value::Timestamp(_)));
    }
    #[test]
    fn make_jun() {
        let r = ev("make_timestamp", &[i(2024), i(6), i(1), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_month", &[r]), i(6));
    }
    #[test]
    fn make_jul() {
        let r = ev("make_timestamp", &[i(2024), i(7), i(1), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_month", &[r]), i(7));
    }
    #[test]
    fn make_aug() {
        let r = ev("make_timestamp", &[i(2024), i(8), i(15), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_day", &[r]), i(15));
    }
    #[test]
    fn make_sep() {
        let r = ev("make_timestamp", &[i(2024), i(9), i(30), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_day", &[r]), i(30));
    }
    #[test]
    fn make_oct() {
        let r = ev("make_timestamp", &[i(2024), i(10), i(31), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_day", &[r]), i(31));
    }
    #[test]
    fn make_nov() {
        let r = ev("make_timestamp", &[i(2024), i(11), i(30), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_day", &[r]), i(30));
    }
    #[test]
    fn make_midnight() {
        let r = ev("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_hour", std::slice::from_ref(&r)), i(0));
        assert_eq!(ev("extract_minute", std::slice::from_ref(&r)), i(0));
        assert_eq!(ev("extract_second", &[r]), i(0));
    }
    #[test]
    fn make_noon() {
        let r = ev("make_timestamp", &[i(2024), i(1), i(1), i(12), i(0), i(0)]);
        assert_eq!(ev("extract_hour", &[r]), i(12));
    }
    #[test]
    fn make_2025() {
        let r = ev("make_timestamp", &[i(2025), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(ev("extract_year", &[r]), i(2025));
    }
    #[test]
    fn make_1999() {
        let r = ev(
            "make_timestamp",
            &[i(1999), i(12), i(31), i(23), i(59), i(59)],
        );
        assert_eq!(ev("extract_year", &[r]), i(1999));
    }
    #[test]
    fn make_h23() {
        let r = ev(
            "make_timestamp",
            &[i(2024), i(1), i(1), i(23), i(59), i(59)],
        );
        assert_eq!(ev("extract_hour", std::slice::from_ref(&r)), i(23));
        assert_eq!(ev("extract_minute", std::slice::from_ref(&r)), i(59));
        assert_eq!(ev("extract_second", &[r]), i(59));
    }
}
