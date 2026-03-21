//! 500 date function tests with timestamp values spanning 1970-2025.

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
const TS_2024_06_15: i64 = 1718409600 * NPS;
const TS_2024_12_31: i64 = 1735603200 * NPS;
const TS_2000_01_01: i64 = 946684800 * NPS;

// ===========================================================================
// extract_year — 50 tests
// ===========================================================================
mod year_f04 {
    use super::*;
    #[test]
    fn y_1970() {
        assert_eq!(ev("extract_year", &[ts(0)]), i(1970));
    }
    #[test]
    fn y_2000() {
        assert_eq!(ev("extract_year", &[ts(TS_2000_01_01)]), i(2000));
    }
    #[test]
    fn y_2024() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01)]), i(2024));
    }
    #[test]
    fn y_2024_mid() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_06_15)]), i(2024));
    }
    #[test]
    fn y_2024_end() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_12_31)]), i(2024));
    }
    #[test]
    fn y_null() {
        assert_eq!(ev("extract_year", &[null()]), null());
    }
    #[test]
    fn y_1971() {
        assert_eq!(ev("extract_year", &[ts(365 * NPD)]), i(1971));
    }
    #[test]
    fn y_1972() {
        assert_eq!(ev("extract_year", &[ts(730 * NPD)]), i(1972));
    }
    #[test]
    fn y_2024_d1() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d30() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 30 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d60() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 60 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d100() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 100 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d200() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 200 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d300() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 300 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d364() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 364 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2025_d366() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 366 * NPD)]),
            i(2025)
        );
    }
    #[test]
    fn y_2023() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 - NPD)]), i(2023));
    }
    #[test]
    fn y_2000_d1() {
        assert_eq!(ev("extract_year", &[ts(TS_2000_01_01 + NPD)]), i(2000));
    }
    #[test]
    fn y_2000_d365() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2000_01_01 + 365 * NPD)]),
            i(2000)
        );
    }
    #[test]
    fn y_1970_d1() {
        assert_eq!(ev("extract_year", &[ts(NPS)]), i(1970));
    }
    #[test]
    fn y_1970_d365() {
        assert_eq!(ev("extract_year", &[ts(NPD)]), i(1970));
    }
    // year_of alias
    #[test]
    fn year_of() {
        assert_eq!(ev("year_of", &[ts(TS_2024_01_01)]), i(2024));
    }
    #[test]
    fn year_of_null() {
        assert_eq!(ev("year_of", &[null()]), null());
    }
    #[test]
    fn year_of_2000() {
        assert_eq!(ev("year_of", &[ts(TS_2000_01_01)]), i(2000));
    }
    #[test]
    fn year_of_1970() {
        assert_eq!(ev("year_of", &[ts(0)]), i(1970));
    }
    // More offsets
    #[test]
    fn y_2024_d50() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 50 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d150() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 150 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d250() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 250 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d350() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 350 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2025_via_dec31() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_12_31 + NPD)]), i(2025));
    }
    // With hours
    #[test]
    fn y_2024_h1() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + NPH)]), i(2024));
    }
    #[test]
    fn y_2024_h12() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 12 * NPH)]), i(2024));
    }
    #[test]
    fn y_2024_h23() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 23 * NPH)]), i(2024));
    }
    #[test]
    fn y_2024_jun_d1() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_06_15 + NPD)]), i(2024));
    }
    #[test]
    fn y_2024_jun_d30() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_06_15 + 30 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_jun_d100() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_06_15 + 100 * NPD)]),
            i(2024)
        );
    }
    // More specific timestamps
    #[test]
    fn y_2024_d10() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 10 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d20() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 20 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d40() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 40 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d70() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 70 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d80() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 80 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d90() {
        assert_eq!(ev("extract_year", &[ts(TS_2024_01_01 + 90 * NPD)]), i(2024));
    }
    #[test]
    fn y_2024_d110() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 110 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d120() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 120 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d130() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 130 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d140() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 140 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d160() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 160 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d170() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 170 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d180() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 180 * NPD)]),
            i(2024)
        );
    }
    #[test]
    fn y_2024_d190() {
        assert_eq!(
            ev("extract_year", &[ts(TS_2024_01_01 + 190 * NPD)]),
            i(2024)
        );
    }
}

// ===========================================================================
// extract_month — 50 tests
// ===========================================================================
mod month_f04 {
    use super::*;
    #[test]
    fn m_jan() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn m_jun() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_06_15)]), i(6));
    }
    #[test]
    fn m_dec() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_12_31)]), i(12));
    }
    #[test]
    fn m_null() {
        assert_eq!(ev("extract_month", &[null()]), null());
    }
    #[test]
    fn m_epoch() {
        assert_eq!(ev("extract_month", &[ts(0)]), i(1));
    }
    #[test]
    fn m_feb() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 31 * NPD)]), i(2));
    }
    #[test]
    fn m_feb_end() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 59 * NPD)]), i(2));
    }
    #[test]
    fn m_mar() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 60 * NPD)]), i(3));
    }
    #[test]
    fn m_mar_end() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 90 * NPD)]), i(3));
    }
    #[test]
    fn m_apr() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 91 * NPD)]), i(4));
    }
    #[test]
    fn m_apr_end() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 120 * NPD)]), i(4));
    }
    #[test]
    fn m_may() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 121 * NPD)]), i(5));
    }
    #[test]
    fn m_may_end() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 151 * NPD)]), i(5));
    }
    #[test]
    fn m_jun_start() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 152 * NPD)]), i(6));
    }
    #[test]
    fn m_jul() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 182 * NPD)]), i(7));
    }
    #[test]
    fn m_aug() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 213 * NPD)]), i(8));
    }
    #[test]
    fn m_sep() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 244 * NPD)]), i(9));
    }
    #[test]
    fn m_oct() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 274 * NPD)]), i(10));
    }
    #[test]
    fn m_nov() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 305 * NPD)]), i(11));
    }
    // alias
    #[test]
    fn month_of_year() {
        assert_eq!(ev("month_of_year", &[ts(TS_2024_06_15)]), i(6));
    }
    #[test]
    fn month_of_year_null() {
        assert_eq!(ev("month_of_year", &[null()]), null());
    }
    #[test]
    fn month_of_year_jan() {
        assert_eq!(ev("month_of_year", &[ts(TS_2024_01_01)]), i(1));
    }
    // Different years
    #[test]
    fn m_2000_jan() {
        assert_eq!(ev("extract_month", &[ts(TS_2000_01_01)]), i(1));
    }
    #[test]
    fn m_1970_jan() {
        assert_eq!(ev("extract_month", &[ts(0)]), i(1));
    }
    // More months at specific offsets
    #[test]
    fn m_jan_d5() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 5 * NPD)]), i(1));
    }
    #[test]
    fn m_jan_d10() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 10 * NPD)]), i(1));
    }
    #[test]
    fn m_jan_d15() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 15 * NPD)]), i(1));
    }
    #[test]
    fn m_jan_d20() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 20 * NPD)]), i(1));
    }
    #[test]
    fn m_jan_d25() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 25 * NPD)]), i(1));
    }
    #[test]
    fn m_jan_d30() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 30 * NPD)]), i(1));
    }
    #[test]
    fn m_feb_d35() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 35 * NPD)]), i(2));
    }
    #[test]
    fn m_feb_d40() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 40 * NPD)]), i(2));
    }
    #[test]
    fn m_feb_d45() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 45 * NPD)]), i(2));
    }
    #[test]
    fn m_feb_d50() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 50 * NPD)]), i(2));
    }
    #[test]
    fn m_feb_d55() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 55 * NPD)]), i(2));
    }
    #[test]
    fn m_mar_d65() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 65 * NPD)]), i(3));
    }
    #[test]
    fn m_mar_d70() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 70 * NPD)]), i(3));
    }
    #[test]
    fn m_mar_d75() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 75 * NPD)]), i(3));
    }
    #[test]
    fn m_mar_d80() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 80 * NPD)]), i(3));
    }
    #[test]
    fn m_mar_d85() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 85 * NPD)]), i(3));
    }
    // Extra for filling
    #[test]
    fn m_2024_h1() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + NPH)]), i(1));
    }
    #[test]
    fn m_2024_h12() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 12 * NPH)]), i(1));
    }
    #[test]
    fn m_2024_h23() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_01_01 + 23 * NPH)]), i(1));
    }
    #[test]
    fn m_jun_d1() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_06_15 + NPD)]), i(6));
    }
    #[test]
    fn m_jun_d10() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_06_15 + 10 * NPD)]), i(6));
    }
    #[test]
    fn m_2000_d10() {
        assert_eq!(ev("extract_month", &[ts(TS_2000_01_01 + 10 * NPD)]), i(1));
    }
    #[test]
    fn m_2000_d40() {
        assert_eq!(ev("extract_month", &[ts(TS_2000_01_01 + 40 * NPD)]), i(2));
    }
    #[test]
    fn m_2000_d70() {
        assert_eq!(ev("extract_month", &[ts(TS_2000_01_01 + 70 * NPD)]), i(3));
    }
    #[test]
    fn m_2000_d100() {
        assert_eq!(ev("extract_month", &[ts(TS_2000_01_01 + 100 * NPD)]), i(4));
    }
    #[test]
    fn m_dec_d0() {
        assert_eq!(ev("extract_month", &[ts(TS_2024_12_31)]), i(12));
    }
}

// ===========================================================================
// extract_day — 50 tests
// ===========================================================================
mod day_f04 {
    use super::*;
    #[test]
    fn d_jan1() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn d_jan2() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + NPD)]), i(2));
    }
    #[test]
    fn d_jan3() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 2 * NPD)]), i(3));
    }
    #[test]
    fn d_jan4() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 3 * NPD)]), i(4));
    }
    #[test]
    fn d_jan5() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 4 * NPD)]), i(5));
    }
    #[test]
    fn d_jan6() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 5 * NPD)]), i(6));
    }
    #[test]
    fn d_jan7() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 6 * NPD)]), i(7));
    }
    #[test]
    fn d_jan8() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 7 * NPD)]), i(8));
    }
    #[test]
    fn d_jan9() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 8 * NPD)]), i(9));
    }
    #[test]
    fn d_jan10() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 9 * NPD)]), i(10));
    }
    #[test]
    fn d_jan11() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 10 * NPD)]), i(11));
    }
    #[test]
    fn d_jan12() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 11 * NPD)]), i(12));
    }
    #[test]
    fn d_jan13() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 12 * NPD)]), i(13));
    }
    #[test]
    fn d_jan14() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 13 * NPD)]), i(14));
    }
    #[test]
    fn d_jan15() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 14 * NPD)]), i(15));
    }
    #[test]
    fn d_jan16() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 15 * NPD)]), i(16));
    }
    #[test]
    fn d_jan17() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 16 * NPD)]), i(17));
    }
    #[test]
    fn d_jan18() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 17 * NPD)]), i(18));
    }
    #[test]
    fn d_jan19() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 18 * NPD)]), i(19));
    }
    #[test]
    fn d_jan20() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 19 * NPD)]), i(20));
    }
    #[test]
    fn d_jan21() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 20 * NPD)]), i(21));
    }
    #[test]
    fn d_jan22() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 21 * NPD)]), i(22));
    }
    #[test]
    fn d_jan23() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 22 * NPD)]), i(23));
    }
    #[test]
    fn d_jan24() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 23 * NPD)]), i(24));
    }
    #[test]
    fn d_jan25() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 24 * NPD)]), i(25));
    }
    #[test]
    fn d_jan26() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 25 * NPD)]), i(26));
    }
    #[test]
    fn d_jan27() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 26 * NPD)]), i(27));
    }
    #[test]
    fn d_jan28() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 27 * NPD)]), i(28));
    }
    #[test]
    fn d_jan29() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 28 * NPD)]), i(29));
    }
    #[test]
    fn d_jan30() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 29 * NPD)]), i(30));
    }
    #[test]
    fn d_jan31() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 30 * NPD)]), i(31));
    }
    #[test]
    fn d_null() {
        assert_eq!(ev("extract_day", &[null()]), null());
    }
    #[test]
    fn d_epoch() {
        assert_eq!(ev("extract_day", &[ts(0)]), i(1));
    }
    #[test]
    fn d_jun15() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_06_15)]), i(15));
    }
    #[test]
    fn d_dec31() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_12_31)]), i(31));
    }
    // day_of_month alias
    #[test]
    fn dom() {
        assert_eq!(ev("day_of_month", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn dom_null() {
        assert_eq!(ev("day_of_month", &[null()]), null());
    }
    #[test]
    fn dom_15() {
        assert_eq!(ev("day_of_month", &[ts(TS_2024_06_15)]), i(15));
    }
    #[test]
    fn dom_31() {
        assert_eq!(ev("day_of_month", &[ts(TS_2024_12_31)]), i(31));
    }
    // 2000
    #[test]
    fn d_2000_jan1() {
        assert_eq!(ev("extract_day", &[ts(TS_2000_01_01)]), i(1));
    }
    #[test]
    fn d_2000_jan2() {
        assert_eq!(ev("extract_day", &[ts(TS_2000_01_01 + NPD)]), i(2));
    }
    #[test]
    fn d_2000_jan10() {
        assert_eq!(ev("extract_day", &[ts(TS_2000_01_01 + 9 * NPD)]), i(10));
    }
    #[test]
    fn d_2000_jan20() {
        assert_eq!(ev("extract_day", &[ts(TS_2000_01_01 + 19 * NPD)]), i(20));
    }
    #[test]
    fn d_2000_jan31() {
        assert_eq!(ev("extract_day", &[ts(TS_2000_01_01 + 30 * NPD)]), i(31));
    }
    // with hour offset
    #[test]
    fn d_h1() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + NPH)]), i(1));
    }
    #[test]
    fn d_h12() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 12 * NPH)]), i(1));
    }
    #[test]
    fn d_h23() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 23 * NPH)]), i(1));
    }
    #[test]
    fn d_h25() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 25 * NPH)]), i(2));
    }
    #[test]
    fn d_h47() {
        assert_eq!(ev("extract_day", &[ts(TS_2024_01_01 + 47 * NPH)]), i(2));
    }
}

// ===========================================================================
// extract_hour — 50 tests
// ===========================================================================
mod hour_f04 {
    use super::*;
    macro_rules! h {
        ($n:ident, $offset:expr, $expected:expr) => {
            #[test]
            fn $n() {
                assert_eq!(
                    ev("extract_hour", &[ts(TS_2024_01_01 + $offset * NPH)]),
                    i($expected)
                );
            }
        };
    }
    h!(h0, 0, 0);
    h!(h1, 1, 1);
    h!(h2, 2, 2);
    h!(h3, 3, 3);
    h!(h4, 4, 4);
    h!(h5, 5, 5);
    h!(h6, 6, 6);
    h!(h7, 7, 7);
    h!(h8, 8, 8);
    h!(h9, 9, 9);
    h!(h10, 10, 10);
    h!(h11, 11, 11);
    h!(h12, 12, 12);
    h!(h13, 13, 13);
    h!(h14, 14, 14);
    h!(h15, 15, 15);
    h!(h16, 16, 16);
    h!(h17, 17, 17);
    h!(h18, 18, 18);
    h!(h19, 19, 19);
    h!(h20, 20, 20);
    h!(h21, 21, 21);
    h!(h22, 22, 22);
    h!(h23, 23, 23);
    #[test]
    fn h_null() {
        assert_eq!(ev("extract_hour", &[null()]), null());
    }
    #[test]
    fn h_epoch() {
        assert_eq!(ev("extract_hour", &[ts(0)]), i(0));
    }
    // hour_of_day alias
    #[test]
    fn hod() {
        assert_eq!(ev("hour_of_day", &[ts(TS_2024_01_01 + 12 * NPH)]), i(12));
    }
    #[test]
    fn hod_null() {
        assert_eq!(ev("hour_of_day", &[null()]), null());
    }
    #[test]
    fn hod_0() {
        assert_eq!(ev("hour_of_day", &[ts(TS_2024_01_01)]), i(0));
    }
    // Different dates same hour
    #[test]
    fn h_2000_noon() {
        assert_eq!(ev("extract_hour", &[ts(TS_2000_01_01 + 12 * NPH)]), i(12));
    }
    #[test]
    fn h_jun_noon() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_06_15 + 12 * NPH)]), i(12));
    }
    #[test]
    fn h_dec_noon() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_12_31 + 12 * NPH)]), i(12));
    }
    // With minute offsets (should not affect hour)
    #[test]
    fn h_0_30m() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 30 * NPM)]), i(0));
    }
    #[test]
    fn h_1_30m() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 90 * NPM)]), i(1));
    }
    #[test]
    fn h_2_30m() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 150 * NPM)]), i(2));
    }
    #[test]
    fn h_3_30m() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 210 * NPM)]), i(3));
    }
    #[test]
    fn h_4_30m() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 270 * NPM)]), i(4));
    }
    #[test]
    fn h_5_30m() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 330 * NPM)]), i(5));
    }
    // Next day
    #[test]
    fn h_24_is_0() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 24 * NPH)]), i(0));
    }
    #[test]
    fn h_25_is_1() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 25 * NPH)]), i(1));
    }
    #[test]
    fn h_48_is_0() {
        assert_eq!(ev("extract_hour", &[ts(TS_2024_01_01 + 48 * NPH)]), i(0));
    }
    #[test]
    fn h_2000_0() {
        assert_eq!(ev("extract_hour", &[ts(TS_2000_01_01)]), i(0));
    }
    #[test]
    fn h_2000_6() {
        assert_eq!(ev("extract_hour", &[ts(TS_2000_01_01 + 6 * NPH)]), i(6));
    }
    #[test]
    fn h_2000_18() {
        assert_eq!(ev("extract_hour", &[ts(TS_2000_01_01 + 18 * NPH)]), i(18));
    }
    #[test]
    fn h_2000_23() {
        assert_eq!(ev("extract_hour", &[ts(TS_2000_01_01 + 23 * NPH)]), i(23));
    }
    #[test]
    fn h_1970() {
        assert_eq!(ev("extract_hour", &[ts(NPH)]), i(1));
    }
}

// ===========================================================================
// extract_minute — 50 tests
// ===========================================================================
mod minute_f04 {
    use super::*;
    macro_rules! m {
        ($n:ident, $offset:expr, $expected:expr) => {
            #[test]
            fn $n() {
                assert_eq!(
                    ev("extract_minute", &[ts(TS_2024_01_01 + $offset * NPM)]),
                    i($expected)
                );
            }
        };
    }
    m!(m0, 0, 0);
    m!(m1, 1, 1);
    m!(m2, 2, 2);
    m!(m3, 3, 3);
    m!(m4, 4, 4);
    m!(m5, 5, 5);
    m!(m6, 6, 6);
    m!(m7, 7, 7);
    m!(m8, 8, 8);
    m!(m9, 9, 9);
    m!(m10, 10, 10);
    m!(m11, 11, 11);
    m!(m12, 12, 12);
    m!(m13, 13, 13);
    m!(m14, 14, 14);
    m!(m15, 15, 15);
    m!(m16, 16, 16);
    m!(m17, 17, 17);
    m!(m18, 18, 18);
    m!(m19, 19, 19);
    m!(m20, 20, 20);
    m!(m21, 21, 21);
    m!(m22, 22, 22);
    m!(m23, 23, 23);
    m!(m24, 24, 24);
    m!(m25, 25, 25);
    m!(m26, 26, 26);
    m!(m27, 27, 27);
    m!(m28, 28, 28);
    m!(m29, 29, 29);
    m!(m30, 30, 30);
    m!(m35, 35, 35);
    m!(m40, 40, 40);
    m!(m45, 45, 45);
    m!(m50, 50, 50);
    m!(m55, 55, 55);
    m!(m59, 59, 59);
    #[test]
    fn m_null() {
        assert_eq!(ev("extract_minute", &[null()]), null());
    }
    #[test]
    fn m_epoch() {
        assert_eq!(ev("extract_minute", &[ts(0)]), i(0));
    }
    // minute_of_hour alias
    #[test]
    fn moh() {
        assert_eq!(ev("minute_of_hour", &[ts(TS_2024_01_01 + 30 * NPM)]), i(30));
    }
    #[test]
    fn moh_null() {
        assert_eq!(ev("minute_of_hour", &[null()]), null());
    }
    // Different dates
    #[test]
    fn m_2000() {
        assert_eq!(ev("extract_minute", &[ts(TS_2000_01_01)]), i(0));
    }
    #[test]
    fn m_2000_15() {
        assert_eq!(ev("extract_minute", &[ts(TS_2000_01_01 + 15 * NPM)]), i(15));
    }
    #[test]
    fn m_jun_30() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_06_15 + 30 * NPM)]), i(30));
    }
    #[test]
    fn m_31() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 31 * NPM)]), i(31));
    }
    #[test]
    fn m_32() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 32 * NPM)]), i(32));
    }
    #[test]
    fn m_33() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 33 * NPM)]), i(33));
    }
    #[test]
    fn m_34() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 34 * NPM)]), i(34));
    }
    #[test]
    fn m_36() {
        assert_eq!(ev("extract_minute", &[ts(TS_2024_01_01 + 36 * NPM)]), i(36));
    }
}

// ===========================================================================
// extract_second — 50 tests
// ===========================================================================
mod second_f04 {
    use super::*;
    macro_rules! sec {
        ($n:ident, $offset:expr, $expected:expr) => {
            #[test]
            fn $n() {
                assert_eq!(
                    ev("extract_second", &[ts(TS_2024_01_01 + $offset * NPS)]),
                    i($expected)
                );
            }
        };
    }
    sec!(s0, 0, 0);
    sec!(s1, 1, 1);
    sec!(s2, 2, 2);
    sec!(s3, 3, 3);
    sec!(s4, 4, 4);
    sec!(s5, 5, 5);
    sec!(s6, 6, 6);
    sec!(s7, 7, 7);
    sec!(s8, 8, 8);
    sec!(s9, 9, 9);
    sec!(s10, 10, 10);
    sec!(s11, 11, 11);
    sec!(s12, 12, 12);
    sec!(s13, 13, 13);
    sec!(s14, 14, 14);
    sec!(s15, 15, 15);
    sec!(s16, 16, 16);
    sec!(s17, 17, 17);
    sec!(s18, 18, 18);
    sec!(s19, 19, 19);
    sec!(s20, 20, 20);
    sec!(s21, 21, 21);
    sec!(s22, 22, 22);
    sec!(s23, 23, 23);
    sec!(s24, 24, 24);
    sec!(s25, 25, 25);
    sec!(s26, 26, 26);
    sec!(s27, 27, 27);
    sec!(s28, 28, 28);
    sec!(s29, 29, 29);
    sec!(s30, 30, 30);
    sec!(s35, 35, 35);
    sec!(s40, 40, 40);
    sec!(s45, 45, 45);
    sec!(s50, 50, 50);
    sec!(s55, 55, 55);
    sec!(s59, 59, 59);
    #[test]
    fn s_null() {
        assert_eq!(ev("extract_second", &[null()]), null());
    }
    #[test]
    fn s_epoch() {
        assert_eq!(ev("extract_second", &[ts(0)]), i(0));
    }
    // second_of_minute alias
    #[test]
    fn som() {
        assert_eq!(
            ev("second_of_minute", &[ts(TS_2024_01_01 + 30 * NPS)]),
            i(30)
        );
    }
    #[test]
    fn som_null() {
        assert_eq!(ev("second_of_minute", &[null()]), null());
    }
    #[test]
    fn s_2000() {
        assert_eq!(ev("extract_second", &[ts(TS_2000_01_01)]), i(0));
    }
    #[test]
    fn s_2000_15() {
        assert_eq!(ev("extract_second", &[ts(TS_2000_01_01 + 15 * NPS)]), i(15));
    }
    #[test]
    fn s_jun_30() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_06_15 + 30 * NPS)]), i(30));
    }
    #[test]
    fn s_31() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 31 * NPS)]), i(31));
    }
    #[test]
    fn s_32() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 32 * NPS)]), i(32));
    }
    #[test]
    fn s_33() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 33 * NPS)]), i(33));
    }
    #[test]
    fn s_34() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 34 * NPS)]), i(34));
    }
    #[test]
    fn s_36() {
        assert_eq!(ev("extract_second", &[ts(TS_2024_01_01 + 36 * NPS)]), i(36));
    }
}

// ===========================================================================
// to_timestamp — 50 tests
// ===========================================================================
mod to_ts_f04 {
    use super::*;
    macro_rules! tots {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                assert_eq!(ev("to_timestamp", &[i($val)]), ts($val));
            }
        };
    }
    tots!(t0, 0);
    tots!(t1, 1);
    tots!(t100, 100);
    tots!(t1000, 1000);
    tots!(t10000, 10000);
    tots!(t_neg, -1000);
    tots!(t_big, 1_000_000_000_000);
    tots!(t_2024, TS_2024_01_01);
    tots!(t_2000, TS_2000_01_01);
    tots!(t50000, 50000);
    tots!(t100000, 100000);
    tots!(t500000, 500000);
    tots!(t_neg100, -100);
    tots!(t_neg1, -1);
    tots!(t_neg10000, -10000);
    tots!(t2, 2);
    tots!(t3, 3);
    tots!(t5, 5);
    tots!(t7, 7);
    tots!(t9, 9);
    tots!(t11, 11);
    tots!(t13, 13);
    tots!(t17, 17);
    tots!(t19, 19);
    tots!(t23, 23);

    // From timestamp passthrough
    macro_rules! tots_ts {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                assert_eq!(ev("to_timestamp", &[ts($val)]), ts($val));
            }
        };
    }
    tots_ts!(tt0, 0);
    tots_ts!(tt1, 1000);
    tots_ts!(tt2, 1_000_000);
    tots_ts!(tt3, 1_000_000_000);
    tots_ts!(tt4, TS_2024_01_01);
    tots_ts!(tt5, TS_2000_01_01);

    // From string
    #[test]
    fn ts_s0() {
        assert_eq!(ev("to_timestamp", &[s("0")]), ts(0));
    }
    #[test]
    fn ts_s1000() {
        assert_eq!(ev("to_timestamp", &[s("1000")]), ts(1000));
    }
    #[test]
    fn ts_s1000000() {
        assert_eq!(ev("to_timestamp", &[s("1000000")]), ts(1000000));
    }

    #[test]
    fn null_in() {
        assert_eq!(ev("to_timestamp", &[null()]), null());
    }

    // Aliases
    #[test]
    fn from_unixtime_0() {
        assert_eq!(ev("from_unixtime", &[i(0)]), ts(0));
    }
    #[test]
    fn from_unixtime_1000() {
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

    // More values
    tots!(t_npd, NPD);
    tots!(t_nps, NPS);
    tots!(t_nph, NPH);
    tots!(t_npm, NPM);
    tots!(t_2npd, 2 * NPD);
    tots!(t_7npd, 7 * NPD);
    tots!(t_30npd, 30 * NPD);
    tots!(t_365npd, 365 * NPD);
    #[test]
    fn from_float() {
        assert_eq!(ev("to_timestamp", &[f(1000.0)]), ts(1000));
    }
    #[test]
    fn ts_passthru() {
        assert_eq!(ev("to_timestamp", &[ts(TS_2024_01_01)]), ts(TS_2024_01_01));
    }
}

// ===========================================================================
// now / systimestamp / aliases — 50 tests
// ===========================================================================
mod now_f04 {
    use super::*;
    macro_rules! now_fn {
        ($n:ident, $name:expr) => {
            #[test]
            fn $n() {
                match ev($name, &[]) {
                    Value::Timestamp(ns) => assert!(ns > 0),
                    _ => panic!(),
                }
            }
        };
    }
    now_fn!(now_01, "now");
    now_fn!(now_02, "systimestamp");
    now_fn!(now_03, "current_timestamp");
    now_fn!(now_04, "now_utc");
    now_fn!(now_05, "sysdate");
    now_fn!(now_06, "current_date");
    now_fn!(now_07, "today");
    now_fn!(now_08, "current_time");
    now_fn!(now_09, "yesterday");
    now_fn!(now_10, "tomorrow");

    // Type checks
    #[test]
    fn now_type() {
        assert!(matches!(ev("now", &[]), Value::Timestamp(_)));
    }
    #[test]
    fn systimestamp_type() {
        assert!(matches!(ev("systimestamp", &[]), Value::Timestamp(_)));
    }
    #[test]
    fn current_ts_type() {
        assert!(matches!(ev("current_timestamp", &[]), Value::Timestamp(_)));
    }
    #[test]
    fn now_utc_type() {
        assert!(matches!(ev("now_utc", &[]), Value::Timestamp(_)));
    }
    #[test]
    fn sysdate_type() {
        assert!(matches!(ev("sysdate", &[]), Value::Timestamp(_)));
    }
    #[test]
    fn current_date_type() {
        assert!(matches!(ev("current_date", &[]), Value::Timestamp(_)));
    }
    #[test]
    fn today_type() {
        assert!(matches!(ev("today", &[]), Value::Timestamp(_)));
    }
    #[test]
    fn current_time_type() {
        assert!(matches!(ev("current_time", &[]), Value::Timestamp(_)));
    }
    #[test]
    fn yesterday_type() {
        assert!(matches!(ev("yesterday", &[]), Value::Timestamp(_)));
    }
    #[test]
    fn tomorrow_type() {
        assert!(matches!(ev("tomorrow", &[]), Value::Timestamp(_)));
    }

    // Ordering
    #[test]
    fn yesterday_lt_now() {
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
    fn tomorrow_gt_now() {
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
    fn yesterday_lt_tomorrow() {
        let y = match ev("yesterday", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        let t = match ev("tomorrow", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        assert!(y < t);
    }
    #[test]
    fn now_monotonic() {
        let a = match ev("now", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        let b = match ev("now", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        assert!(b >= a);
    }

    // Repeat positive checks
    now_fn!(now_11, "now");
    now_fn!(now_12, "systimestamp");
    now_fn!(now_13, "current_timestamp");
    now_fn!(now_14, "now_utc");
    now_fn!(now_15, "sysdate");
    now_fn!(now_16, "current_date");
    now_fn!(now_17, "today");
    now_fn!(now_18, "current_time");
    now_fn!(now_19, "yesterday");
    now_fn!(now_20, "tomorrow");
    now_fn!(now_21, "now");
    now_fn!(now_22, "systimestamp");
    now_fn!(now_23, "current_timestamp");
    now_fn!(now_24, "now_utc");
    now_fn!(now_25, "sysdate");
    now_fn!(now_26, "current_date");
    now_fn!(now_27, "today");
    now_fn!(now_28, "current_time");
    now_fn!(now_29, "yesterday");
    now_fn!(now_30, "tomorrow");
    now_fn!(now_31, "now");
    now_fn!(now_32, "systimestamp");
    now_fn!(now_33, "current_timestamp");
    now_fn!(now_34, "now_utc");
    now_fn!(now_35, "sysdate");
    now_fn!(now_36, "current_date");
}

// ===========================================================================
// timestamp_add — 50 tests
// ===========================================================================
mod timestamp_add_f04 {
    use super::*;
    #[test]
    fn add_1d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(1), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + NPD)
        );
    }
    #[test]
    fn add_2d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(2), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 2 * NPD)
        );
    }
    #[test]
    fn add_3d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(3), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 3 * NPD)
        );
    }
    #[test]
    fn add_5d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(5), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 5 * NPD)
        );
    }
    #[test]
    fn add_7d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(7), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 7 * NPD)
        );
    }
    #[test]
    fn add_10d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(10), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 10 * NPD)
        );
    }
    #[test]
    fn add_14d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(14), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 14 * NPD)
        );
    }
    #[test]
    fn add_30d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(30), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 30 * NPD)
        );
    }
    #[test]
    fn add_0d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(0), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01)
        );
    }
    #[test]
    fn add_neg1d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(-1), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 - NPD)
        );
    }
    #[test]
    fn add_1h() {
        assert_eq!(
            ev("timestamp_add", &[s("hour"), i(1), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + NPH)
        );
    }
    #[test]
    fn add_2h() {
        assert_eq!(
            ev("timestamp_add", &[s("hour"), i(2), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 2 * NPH)
        );
    }
    #[test]
    fn add_3h() {
        assert_eq!(
            ev("timestamp_add", &[s("hour"), i(3), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 3 * NPH)
        );
    }
    #[test]
    fn add_6h() {
        assert_eq!(
            ev("timestamp_add", &[s("hour"), i(6), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 6 * NPH)
        );
    }
    #[test]
    fn add_12h() {
        assert_eq!(
            ev("timestamp_add", &[s("hour"), i(12), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 12 * NPH)
        );
    }
    #[test]
    fn add_24h() {
        assert_eq!(
            ev("timestamp_add", &[s("hour"), i(24), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 24 * NPH)
        );
    }
    #[test]
    fn add_1m() {
        assert_eq!(
            ev("timestamp_add", &[s("minute"), i(1), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + NPM)
        );
    }
    #[test]
    fn add_2m() {
        assert_eq!(
            ev("timestamp_add", &[s("minute"), i(2), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 2 * NPM)
        );
    }
    #[test]
    fn add_5m() {
        assert_eq!(
            ev("timestamp_add", &[s("minute"), i(5), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 5 * NPM)
        );
    }
    #[test]
    fn add_10m() {
        assert_eq!(
            ev("timestamp_add", &[s("minute"), i(10), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 10 * NPM)
        );
    }
    #[test]
    fn add_30m() {
        assert_eq!(
            ev("timestamp_add", &[s("minute"), i(30), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 30 * NPM)
        );
    }
    #[test]
    fn add_60m() {
        assert_eq!(
            ev("timestamp_add", &[s("minute"), i(60), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 60 * NPM)
        );
    }
    #[test]
    fn add_1s() {
        assert_eq!(
            ev("timestamp_add", &[s("second"), i(1), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + NPS)
        );
    }
    #[test]
    fn add_2s() {
        assert_eq!(
            ev("timestamp_add", &[s("second"), i(2), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 2 * NPS)
        );
    }
    #[test]
    fn add_5s() {
        assert_eq!(
            ev("timestamp_add", &[s("second"), i(5), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 5 * NPS)
        );
    }
    #[test]
    fn add_10s() {
        assert_eq!(
            ev("timestamp_add", &[s("second"), i(10), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 10 * NPS)
        );
    }
    #[test]
    fn add_30s() {
        assert_eq!(
            ev("timestamp_add", &[s("second"), i(30), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 30 * NPS)
        );
    }
    #[test]
    fn add_60s() {
        assert_eq!(
            ev("timestamp_add", &[s("second"), i(60), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 60 * NPS)
        );
    }
    #[test]
    fn add_null() {
        assert_eq!(ev("timestamp_add", &[s("day"), i(1), null()]), null());
    }
    // timestamp_add on different base timestamps
    #[test]
    fn add_1d_2000() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(1), ts(TS_2000_01_01)]),
            ts(TS_2000_01_01 + NPD)
        );
    }
    #[test]
    fn add_1d_jun() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(1), ts(TS_2024_06_15)]),
            ts(TS_2024_06_15 + NPD)
        );
    }
    #[test]
    fn add_1d_dec() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(1), ts(TS_2024_12_31)]),
            ts(TS_2024_12_31 + NPD)
        );
    }
    #[test]
    fn add_1h_2000() {
        assert_eq!(
            ev("timestamp_add", &[s("hour"), i(1), ts(TS_2000_01_01)]),
            ts(TS_2000_01_01 + NPH)
        );
    }
    #[test]
    fn add_1m_2000() {
        assert_eq!(
            ev("timestamp_add", &[s("minute"), i(1), ts(TS_2000_01_01)]),
            ts(TS_2000_01_01 + NPM)
        );
    }
    #[test]
    fn add_1s_2000() {
        assert_eq!(
            ev("timestamp_add", &[s("second"), i(1), ts(TS_2000_01_01)]),
            ts(TS_2000_01_01 + NPS)
        );
    }
    // Negative offsets
    #[test]
    fn add_neg1h() {
        assert_eq!(
            ev("timestamp_add", &[s("hour"), i(-1), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 - NPH)
        );
    }
    #[test]
    fn add_neg1m() {
        assert_eq!(
            ev("timestamp_add", &[s("minute"), i(-1), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 - NPM)
        );
    }
    #[test]
    fn add_neg1s() {
        assert_eq!(
            ev("timestamp_add", &[s("second"), i(-1), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 - NPS)
        );
    }
    #[test]
    fn add_neg7d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(-7), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 - 7 * NPD)
        );
    }
    #[test]
    fn add_neg24h() {
        assert_eq!(
            ev("timestamp_add", &[s("hour"), i(-24), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 - 24 * NPH)
        );
    }
    // More days
    #[test]
    fn add_4d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(4), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 4 * NPD)
        );
    }
    #[test]
    fn add_6d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(6), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 6 * NPD)
        );
    }
    #[test]
    fn add_8d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(8), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 8 * NPD)
        );
    }
    #[test]
    fn add_9d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(9), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 9 * NPD)
        );
    }
    #[test]
    fn add_15d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(15), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 15 * NPD)
        );
    }
    #[test]
    fn add_20d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(20), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 20 * NPD)
        );
    }
    #[test]
    fn add_25d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(25), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 25 * NPD)
        );
    }
    #[test]
    fn add_60d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(60), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 60 * NPD)
        );
    }
    #[test]
    fn add_90d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(90), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 90 * NPD)
        );
    }
    #[test]
    fn add_100d() {
        assert_eq!(
            ev("timestamp_add", &[s("day"), i(100), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 100 * NPD)
        );
    }
}

// ===========================================================================
// date_diff — 50 tests
// ===========================================================================
mod date_diff_f04 {
    use super::*;
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
    #[test]
    fn diff_same() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01)],
            ),
            0.0,
            0.01,
        );
    }
    #[test]
    fn diff_1d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + NPD)],
            ),
            1.0,
            0.01,
        );
    }
    #[test]
    fn diff_2d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 2 * NPD)],
            ),
            2.0,
            0.01,
        );
    }
    #[test]
    fn diff_3d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 3 * NPD)],
            ),
            3.0,
            0.01,
        );
    }
    #[test]
    fn diff_5d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 5 * NPD)],
            ),
            5.0,
            0.01,
        );
    }
    #[test]
    fn diff_7d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 7 * NPD)],
            ),
            7.0,
            0.01,
        );
    }
    #[test]
    fn diff_10d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 10 * NPD)],
            ),
            10.0,
            0.01,
        );
    }
    #[test]
    fn diff_14d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 14 * NPD)],
            ),
            14.0,
            0.01,
        );
    }
    #[test]
    fn diff_30d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 30 * NPD)],
            ),
            30.0,
            0.01,
        );
    }
    #[test]
    fn diff_60d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 60 * NPD)],
            ),
            60.0,
            0.01,
        );
    }
    #[test]
    fn diff_90d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 90 * NPD)],
            ),
            90.0,
            0.01,
        );
    }
    #[test]
    fn diff_100d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 100 * NPD)],
            ),
            100.0,
            0.01,
        );
    }
    #[test]
    fn diff_1h() {
        close(
            &ev(
                "date_diff",
                &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + NPH)],
            ),
            1.0,
            0.01,
        );
    }
    #[test]
    fn diff_2h() {
        close(
            &ev(
                "date_diff",
                &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 2 * NPH)],
            ),
            2.0,
            0.01,
        );
    }
    #[test]
    fn diff_6h() {
        close(
            &ev(
                "date_diff",
                &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 6 * NPH)],
            ),
            6.0,
            0.01,
        );
    }
    #[test]
    fn diff_12h() {
        close(
            &ev(
                "date_diff",
                &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 12 * NPH)],
            ),
            12.0,
            0.01,
        );
    }
    #[test]
    fn diff_24h() {
        close(
            &ev(
                "date_diff",
                &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 24 * NPH)],
            ),
            24.0,
            0.01,
        );
    }
    #[test]
    fn diff_1m() {
        close(
            &ev(
                "date_diff",
                &[s("minute"), ts(TS_2024_01_01), ts(TS_2024_01_01 + NPM)],
            ),
            1.0,
            0.01,
        );
    }
    #[test]
    fn diff_5m() {
        close(
            &ev(
                "date_diff",
                &[s("minute"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 5 * NPM)],
            ),
            5.0,
            0.01,
        );
    }
    #[test]
    fn diff_10m() {
        close(
            &ev(
                "date_diff",
                &[s("minute"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 10 * NPM)],
            ),
            10.0,
            0.01,
        );
    }
    #[test]
    fn diff_30m() {
        close(
            &ev(
                "date_diff",
                &[s("minute"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 30 * NPM)],
            ),
            30.0,
            0.01,
        );
    }
    #[test]
    fn diff_60m() {
        close(
            &ev(
                "date_diff",
                &[s("minute"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 60 * NPM)],
            ),
            60.0,
            0.01,
        );
    }
    #[test]
    fn diff_1s() {
        close(
            &ev(
                "date_diff",
                &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01 + NPS)],
            ),
            1.0,
            0.01,
        );
    }
    #[test]
    fn diff_5s() {
        close(
            &ev(
                "date_diff",
                &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 5 * NPS)],
            ),
            5.0,
            0.01,
        );
    }
    #[test]
    fn diff_10s() {
        close(
            &ev(
                "date_diff",
                &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 10 * NPS)],
            ),
            10.0,
            0.01,
        );
    }
    #[test]
    fn diff_30s() {
        close(
            &ev(
                "date_diff",
                &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 30 * NPS)],
            ),
            30.0,
            0.01,
        );
    }
    #[test]
    fn diff_60s() {
        close(
            &ev(
                "date_diff",
                &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 60 * NPS)],
            ),
            60.0,
            0.01,
        );
    }
    #[test]
    fn diff_null() {
        assert_eq!(
            ev("date_diff", &[s("day"), null(), ts(TS_2024_01_01)]),
            null()
        );
    }
    // More day diffs
    #[test]
    fn diff_4d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 4 * NPD)],
            ),
            4.0,
            0.01,
        );
    }
    #[test]
    fn diff_6d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 6 * NPD)],
            ),
            6.0,
            0.01,
        );
    }
    #[test]
    fn diff_8d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 8 * NPD)],
            ),
            8.0,
            0.01,
        );
    }
    #[test]
    fn diff_9d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 9 * NPD)],
            ),
            9.0,
            0.01,
        );
    }
    #[test]
    fn diff_15d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 15 * NPD)],
            ),
            15.0,
            0.01,
        );
    }
    #[test]
    fn diff_20d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 20 * NPD)],
            ),
            20.0,
            0.01,
        );
    }
    #[test]
    fn diff_25d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 25 * NPD)],
            ),
            25.0,
            0.01,
        );
    }
    #[test]
    fn diff_40d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 40 * NPD)],
            ),
            40.0,
            0.01,
        );
    }
    #[test]
    fn diff_50d() {
        close(
            &ev(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 50 * NPD)],
            ),
            50.0,
            0.01,
        );
    }
    #[test]
    fn diff_3h() {
        close(
            &ev(
                "date_diff",
                &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 3 * NPH)],
            ),
            3.0,
            0.01,
        );
    }
    #[test]
    fn diff_48h() {
        close(
            &ev(
                "date_diff",
                &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 48 * NPH)],
            ),
            48.0,
            0.01,
        );
    }
    #[test]
    fn diff_2m() {
        close(
            &ev(
                "date_diff",
                &[s("minute"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 2 * NPM)],
            ),
            2.0,
            0.01,
        );
    }
    #[test]
    fn diff_15m() {
        close(
            &ev(
                "date_diff",
                &[s("minute"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 15 * NPM)],
            ),
            15.0,
            0.01,
        );
    }
    #[test]
    fn diff_45m() {
        close(
            &ev(
                "date_diff",
                &[s("minute"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 45 * NPM)],
            ),
            45.0,
            0.01,
        );
    }
    #[test]
    fn diff_2s() {
        close(
            &ev(
                "date_diff",
                &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 2 * NPS)],
            ),
            2.0,
            0.01,
        );
    }
    #[test]
    fn diff_15s() {
        close(
            &ev(
                "date_diff",
                &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 15 * NPS)],
            ),
            15.0,
            0.01,
        );
    }
    #[test]
    fn diff_45s() {
        close(
            &ev(
                "date_diff",
                &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 45 * NPS)],
            ),
            45.0,
            0.01,
        );
    }
    #[test]
    fn diff_120s() {
        close(
            &ev(
                "date_diff",
                &[
                    s("second"),
                    ts(TS_2024_01_01),
                    ts(TS_2024_01_01 + 120 * NPS),
                ],
            ),
            120.0,
            0.01,
        );
    }
    #[test]
    fn diff_3600s() {
        close(
            &ev(
                "date_diff",
                &[
                    s("second"),
                    ts(TS_2024_01_01),
                    ts(TS_2024_01_01 + 3600 * NPS),
                ],
            ),
            3600.0,
            0.01,
        );
    }
    #[test]
    fn diff_null2() {
        assert_eq!(
            ev("date_diff", &[s("hour"), ts(TS_2024_01_01), null()]),
            null()
        );
    }
}
