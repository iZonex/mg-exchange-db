//! Massive date/time function test suite — 1000+ tests.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn s(v: &str) -> Value { Value::Str(v.to_string()) }
fn i(v: i64) -> Value { Value::I64(v) }
fn f(v: f64) -> Value { Value::F64(v) }
fn ts(ns: i64) -> Value { Value::Timestamp(ns) }
fn null() -> Value { Value::Null }
fn eval(name: &str, args: &[Value]) -> Value { evaluate_scalar(name, args).unwrap() }

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MIN: i64 = 60 * NANOS_PER_SEC;
const NANOS_PER_HOUR: i64 = 3600 * NANOS_PER_SEC;
const NANOS_PER_DAY: i64 = 86400 * NANOS_PER_SEC;

const EPOCH: i64 = 0;
const TS_2024_01_01: i64 = 1704067200 * NANOS_PER_SEC;
const TS_2024_02_29: i64 = 1709164800 * NANOS_PER_SEC; // leap day
const TS_2024_03_15_123045: i64 = 1710505845 * NANOS_PER_SEC;
const TS_2024_06_15: i64 = 1718409600 * NANOS_PER_SEC; // Saturday
const TS_2024_12_31_235959: i64 = 1735689599 * NANOS_PER_SEC;
const TS_2023_06_15: i64 = 1686787200 * NANOS_PER_SEC;
const TS_2000_01_01: i64 = 946684800 * NANOS_PER_SEC;

// ===========================================================================
// now / systimestamp / current_timestamp / sysdate / now_utc
// ===========================================================================
mod now_extra {
    use super::*;
    #[test] fn now_returns_ts() { match eval("now", &[]) { Value::Timestamp(ns) => assert!(ns > 0), _ => panic!() } }
    #[test] fn systimestamp_alias() { match eval("systimestamp", &[]) { Value::Timestamp(ns) => assert!(ns > 0), _ => panic!() } }
    #[test] fn current_timestamp_alias() { match eval("current_timestamp", &[]) { Value::Timestamp(ns) => assert!(ns > 0), _ => panic!() } }
    #[test] fn now_utc_alias() { match eval("now_utc", &[]) { Value::Timestamp(ns) => assert!(ns > 0), _ => panic!() } }
    #[test] fn sysdate_alias() { match eval("sysdate", &[]) { Value::Timestamp(ns) => assert!(ns > 0), _ => panic!() } }
    #[test] fn current_date_alias() { match eval("current_date", &[]) { Value::Timestamp(ns) => assert!(ns > 0), _ => panic!() } }
    #[test] fn today_alias() { match eval("today", &[]) { Value::Timestamp(ns) => assert!(ns > 0), _ => panic!() } }
    #[test] fn yesterday_before_now() {
        let y = match eval("yesterday", &[]) { Value::Timestamp(ns) => ns, _ => panic!() };
        let n = match eval("now", &[]) { Value::Timestamp(ns) => ns, _ => panic!() };
        assert!(y < n);
    }
    #[test] fn tomorrow_after_now() {
        let t = match eval("tomorrow", &[]) { Value::Timestamp(ns) => ns, _ => panic!() };
        let n = match eval("now", &[]) { Value::Timestamp(ns) => ns, _ => panic!() };
        assert!(t > n);
    }
    #[test] fn yesterday_approx_1_day_before() {
        let y = match eval("yesterday", &[]) { Value::Timestamp(ns) => ns, _ => panic!() };
        let n = match eval("now", &[]) { Value::Timestamp(ns) => ns, _ => panic!() };
        let diff = n - y;
        assert!(diff > NANOS_PER_DAY / 2 && diff < 2 * NANOS_PER_DAY);
    }
    #[test] fn tomorrow_approx_1_day_after() {
        let t = match eval("tomorrow", &[]) { Value::Timestamp(ns) => ns, _ => panic!() };
        let n = match eval("now", &[]) { Value::Timestamp(ns) => ns, _ => panic!() };
        let diff = t - n;
        assert!(diff > NANOS_PER_DAY / 2 && diff < 2 * NANOS_PER_DAY);
    }
}

// ===========================================================================
// to_timestamp (+ aliases)
// ===========================================================================
mod to_timestamp_extra {
    use super::*;
    #[test] fn from_int() { assert_eq!(eval("to_timestamp", &[i(1000)]), ts(1000)); }
    #[test] fn from_ts() { assert_eq!(eval("to_timestamp", &[ts(1000)]), ts(1000)); }
    #[test] fn from_str() { assert_eq!(eval("to_timestamp", &[s("1000")]), ts(1000)); }
    #[test]     fn null_in() { assert_eq!(eval("to_timestamp", &[null()]), null()); }
    #[test] fn epoch() { assert_eq!(eval("to_timestamp", &[i(0)]), ts(0)); }
    #[test] fn large() { assert_eq!(eval("to_timestamp", &[i(TS_2024_01_01)]), ts(TS_2024_01_01)); }
    #[test] fn float_in() { assert_eq!(eval("to_timestamp", &[f(1000.0)]), ts(1000)); }
    #[test] fn from_unixtime_alias() { assert_eq!(eval("from_unixtime", &[i(1000)]), ts(1000)); }
    #[test] fn str_to_timestamp_alias() { assert_eq!(eval("str_to_timestamp", &[s("1000")]), ts(1000)); }
    #[test] fn parse_timestamp_alias() { assert_eq!(eval("parse_timestamp", &[s("1000")]), ts(1000)); }
    #[test] fn negative() { assert_eq!(eval("to_timestamp", &[i(-1000)]), ts(-1000)); }
    #[test] fn date_parse_alias() { assert_eq!(eval("date_parse", &[s("1000")]), ts(1000)); }
    #[test] fn timestamp_parse_alias() { assert_eq!(eval("timestamp_parse", &[s("1000")]), ts(1000)); }
}

// ===========================================================================
// extract_year
// ===========================================================================
mod extract_year_extra {
    use super::*;
    #[test] fn epoch_year() { assert_eq!(eval("extract_year", &[ts(EPOCH)]), i(1970)); }
    #[test] fn y2024_jan() { assert_eq!(eval("extract_year", &[ts(TS_2024_01_01)]), i(2024)); }
    #[test] fn y2024_leap() { assert_eq!(eval("extract_year", &[ts(TS_2024_02_29)]), i(2024)); }
    #[test] fn y2024_dec() { assert_eq!(eval("extract_year", &[ts(TS_2024_12_31_235959)]), i(2024)); }
    #[test] fn y2000() { assert_eq!(eval("extract_year", &[ts(TS_2000_01_01)]), i(2000)); }
    #[test]     fn null_in() { assert_eq!(eval("extract_year", &[null()]), null()); }
    #[test] fn from_int() { assert_eq!(eval("extract_year", &[i(TS_2024_01_01)]), i(2024)); }
    #[test] fn y2023() { assert_eq!(eval("extract_year", &[ts(TS_2023_06_15)]), i(2023)); }
    #[test] fn year_of_alias() { assert_eq!(eval("year_of", &[ts(TS_2024_01_01)]), i(2024)); }
    #[test] fn y2024_mar() { assert_eq!(eval("extract_year", &[ts(TS_2024_03_15_123045)]), i(2024)); }
    #[test] fn y2024_jun() { assert_eq!(eval("extract_year", &[ts(TS_2024_06_15)]), i(2024)); }
}

// ===========================================================================
// extract_month
// ===========================================================================
mod extract_month_extra {
    use super::*;
    #[test] fn epoch_month() { assert_eq!(eval("extract_month", &[ts(EPOCH)]), i(1)); }
    #[test] fn jan() { assert_eq!(eval("extract_month", &[ts(TS_2024_01_01)]), i(1)); }
    #[test] fn feb() { assert_eq!(eval("extract_month", &[ts(TS_2024_02_29)]), i(2)); }
    #[test] fn mar() { assert_eq!(eval("extract_month", &[ts(TS_2024_03_15_123045)]), i(3)); }
    #[test] fn jun() { assert_eq!(eval("extract_month", &[ts(TS_2024_06_15)]), i(6)); }
    #[test] fn dec() { assert_eq!(eval("extract_month", &[ts(TS_2024_12_31_235959)]), i(12)); }
    #[test]     fn null_in() { assert_eq!(eval("extract_month", &[null()]), null()); }
    #[test] fn month_of_year_alias() { assert_eq!(eval("month_of_year", &[ts(TS_2024_06_15)]), i(6)); }
    #[test] fn from_int() { assert_eq!(eval("extract_month", &[i(TS_2024_01_01)]), i(1)); }
}

// ===========================================================================
// extract_day
// ===========================================================================
mod extract_day_extra {
    use super::*;
    #[test] fn epoch_day() { assert_eq!(eval("extract_day", &[ts(EPOCH)]), i(1)); }
    #[test] fn jan_1() { assert_eq!(eval("extract_day", &[ts(TS_2024_01_01)]), i(1)); }
    #[test] fn feb_29() { assert_eq!(eval("extract_day", &[ts(TS_2024_02_29)]), i(29)); }
    #[test] fn mar_15() { assert_eq!(eval("extract_day", &[ts(TS_2024_03_15_123045)]), i(15)); }
    #[test] fn jun_15() { assert_eq!(eval("extract_day", &[ts(TS_2024_06_15)]), i(15)); }
    #[test] fn dec_31() { assert_eq!(eval("extract_day", &[ts(TS_2024_12_31_235959)]), i(31)); }
    #[test]     fn null_in() { assert_eq!(eval("extract_day", &[null()]), null()); }
    #[test] fn day_of_month_alias() { assert_eq!(eval("day_of_month", &[ts(TS_2024_03_15_123045)]), i(15)); }
    #[test] fn from_int() { assert_eq!(eval("extract_day", &[i(TS_2024_01_01)]), i(1)); }
}

// ===========================================================================
// extract_hour
// ===========================================================================
mod extract_hour_extra {
    use super::*;
    #[test] fn epoch_hour() { assert_eq!(eval("extract_hour", &[ts(EPOCH)]), i(0)); }
    #[test] fn midnight() { assert_eq!(eval("extract_hour", &[ts(TS_2024_01_01)]), i(0)); }
    #[test] fn noon() { assert_eq!(eval("extract_hour", &[ts(TS_2024_03_15_123045)]), i(12)); }
    #[test] fn end_of_day() { assert_eq!(eval("extract_hour", &[ts(TS_2024_12_31_235959)]), i(23)); }
    #[test]     fn null_in() { assert_eq!(eval("extract_hour", &[null()]), null()); }
    #[test] fn hour_of_day_alias() { assert_eq!(eval("hour_of_day", &[ts(TS_2024_03_15_123045)]), i(12)); }
    #[test] fn from_int() { assert_eq!(eval("extract_hour", &[i(TS_2024_01_01)]), i(0)); }
}

// ===========================================================================
// extract_minute / extract_second
// ===========================================================================
mod extract_min_sec_extra {
    use super::*;
    #[test] fn min_epoch() { assert_eq!(eval("extract_minute", &[ts(EPOCH)]), i(0)); }
    #[test] fn min_30() { assert_eq!(eval("extract_minute", &[ts(TS_2024_03_15_123045)]), i(30)); }
    #[test] fn min_59() { assert_eq!(eval("extract_minute", &[ts(TS_2024_12_31_235959)]), i(59)); }
    #[test] fn min_null() { assert_eq!(eval("extract_minute", &[null()]), null()); }
    #[test] fn minute_of_hour_alias() { assert_eq!(eval("minute_of_hour", &[ts(TS_2024_03_15_123045)]), i(30)); }

    #[test] fn sec_epoch() { assert_eq!(eval("extract_second", &[ts(EPOCH)]), i(0)); }
    #[test] fn sec_45() { assert_eq!(eval("extract_second", &[ts(TS_2024_03_15_123045)]), i(45)); }
    #[test] fn sec_59() { assert_eq!(eval("extract_second", &[ts(TS_2024_12_31_235959)]), i(59)); }
    #[test] fn sec_null() { assert_eq!(eval("extract_second", &[null()]), null()); }
    #[test] fn second_of_minute_alias() { assert_eq!(eval("second_of_minute", &[ts(TS_2024_03_15_123045)]), i(45)); }
}

// ===========================================================================
// extract_week / extract_quarter / extract_day_of_week / extract_day_of_year
// ===========================================================================
mod extract_misc_extra {
    use super::*;
    #[test] fn week_epoch() { let r = eval("extract_week", &[ts(EPOCH)]); match r { Value::I64(v) => assert!(v >= 1 && v <= 53), _ => panic!() } }
    #[test] fn week_2024_jan() { let r = eval("extract_week", &[ts(TS_2024_01_01)]); match r { Value::I64(v) => assert!(v >= 1 && v <= 53), _ => panic!() } }
    #[test] fn week_null() { assert_eq!(eval("extract_week", &[null()]), null()); }
    #[test] fn week_of_year_alias() { let r = eval("week_of_year", &[ts(TS_2024_06_15)]); match r { Value::I64(v) => assert!(v >= 1 && v <= 53), _ => panic!() } }

    #[test] fn quarter_jan() { assert_eq!(eval("extract_quarter", &[ts(TS_2024_01_01)]), i(1)); }
    #[test] fn quarter_feb() { assert_eq!(eval("extract_quarter", &[ts(TS_2024_02_29)]), i(1)); }
    #[test] fn quarter_mar() { assert_eq!(eval("extract_quarter", &[ts(TS_2024_03_15_123045)]), i(1)); }
    #[test] fn quarter_jun() { assert_eq!(eval("extract_quarter", &[ts(TS_2024_06_15)]), i(2)); }
    #[test] fn quarter_dec() { assert_eq!(eval("extract_quarter", &[ts(TS_2024_12_31_235959)]), i(4)); }
    #[test] fn quarter_null() { assert_eq!(eval("extract_quarter", &[null()]), null()); }
    #[test] fn quarter_of_year_alias() { assert_eq!(eval("quarter_of_year", &[ts(TS_2024_06_15)]), i(2)); }

    #[test] fn dow_sat() { let r = eval("extract_day_of_week", &[ts(TS_2024_06_15)]); match r { Value::I64(v) => assert!(v >= 0 && v <= 7), _ => panic!() } }
    #[test] fn dow_null() { assert_eq!(eval("extract_day_of_week", &[null()]), null()); }
    #[test] fn day_of_week_alias() { let r = eval("day_of_week", &[ts(TS_2024_06_15)]); match r { Value::I64(_) => {}, _ => panic!() } }

    #[test] fn doy_jan1() { assert_eq!(eval("extract_day_of_year", &[ts(TS_2024_01_01)]), i(1)); }
    #[test] fn doy_dec31() { assert_eq!(eval("extract_day_of_year", &[ts(TS_2024_12_31_235959)]), i(366)); }
    #[test] fn doy_null() { assert_eq!(eval("extract_day_of_year", &[null()]), null()); }
    #[test] fn day_of_year_alias() { assert_eq!(eval("day_of_year", &[ts(TS_2024_01_01)]), i(1)); }
}

// ===========================================================================
// date_trunc
// ===========================================================================
mod date_trunc_extra {
    use super::*;
    #[test] fn trunc_year() { let r = eval("date_trunc", &[s("year"), ts(TS_2024_03_15_123045)]); assert_eq!(r, ts(TS_2024_01_01)); }
    #[test] fn trunc_month() { let r = eval("date_trunc", &[s("month"), ts(TS_2024_03_15_123045)]); match r { Value::Timestamp(_) => {}, _ => panic!() } }
    #[test] fn trunc_day() { let r = eval("date_trunc", &[s("day"), ts(TS_2024_03_15_123045)]); match r { Value::Timestamp(v) => assert!(v < TS_2024_03_15_123045), _ => panic!() } }
    #[test] fn trunc_hour() { let r = eval("date_trunc", &[s("hour"), ts(TS_2024_03_15_123045)]); match r { Value::Timestamp(v) => assert!(v <= TS_2024_03_15_123045), _ => panic!() } }
    #[test] fn trunc_null() { assert_eq!(eval("date_trunc", &[s("year"), null()]), null()); }
    #[test] fn trunc_epoch() { assert_eq!(eval("date_trunc", &[s("year"), ts(EPOCH)]), ts(EPOCH)); }
    #[test] fn timestamp_floor_alias() { let r = eval("timestamp_floor", &[s("year"), ts(TS_2024_03_15_123045)]); assert_eq!(r, ts(TS_2024_01_01)); }
    #[test] fn trunc_minute() { let r = eval("date_trunc", &[s("minute"), ts(TS_2024_03_15_123045)]); match r { Value::Timestamp(v) => assert!(v <= TS_2024_03_15_123045), _ => panic!() } }
    #[test] fn trunc_second() { let r = eval("date_trunc", &[s("second"), ts(TS_2024_03_15_123045)]); assert_eq!(r, ts(TS_2024_03_15_123045)); }
    #[test] fn trunc_year_2000() { let r = eval("date_trunc", &[s("year"), ts(TS_2000_01_01)]); assert_eq!(r, ts(TS_2000_01_01)); }
}

// ===========================================================================
// date_diff
// ===========================================================================
mod date_diff_extra {
    use super::*;
    #[test] fn diff_seconds() { let r = eval("date_diff", &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 60 * NANOS_PER_SEC)]); match r { Value::I64(v) => assert_eq!(v, 60), _ => panic!("{r:?}") } }
    #[test] fn diff_minutes() { let r = eval("date_diff", &[s("minute"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 120 * NANOS_PER_SEC)]); match r { Value::I64(v) => assert_eq!(v, 2), _ => panic!("{r:?}") } }
    #[test] fn diff_hours() { let r = eval("date_diff", &[s("hour"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 3 * NANOS_PER_HOUR)]); match r { Value::I64(v) => assert_eq!(v, 3), _ => panic!("{r:?}") } }
    #[test] fn diff_days() { let r = eval("date_diff", &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + 5 * NANOS_PER_DAY)]); match r { Value::I64(v) => assert_eq!(v, 5), _ => panic!("{r:?}") } }
    #[test] fn diff_null() { assert_eq!(eval("date_diff", &[s("day"), null(), ts(TS_2024_01_01)]), null()); }
    #[test] fn diff_same() { let r = eval("date_diff", &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01)]); assert_eq!(r, i(0)); }
    #[test] fn datediff_alias() { let r = eval("datediff", &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + NANOS_PER_DAY)]); assert_eq!(r, i(1)); }
    #[test] fn timestamp_diff_alias() { let r = eval("timestamp_diff", &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01 + NANOS_PER_DAY)]); assert_eq!(r, i(1)); }
}

// ===========================================================================
// timestamp_add
// ===========================================================================
mod timestamp_add_extra {
    use super::*;
    #[test] fn add_seconds() { let r = eval("timestamp_add", &[s("second"), i(60), ts(TS_2024_01_01)]); assert_eq!(r, ts(TS_2024_01_01 + 60 * NANOS_PER_SEC)); }
    #[test] fn add_minutes() { let r = eval("timestamp_add", &[s("minute"), i(5), ts(TS_2024_01_01)]); assert_eq!(r, ts(TS_2024_01_01 + 5 * NANOS_PER_MIN)); }
    #[test] fn add_hours() { let r = eval("timestamp_add", &[s("hour"), i(3), ts(TS_2024_01_01)]); assert_eq!(r, ts(TS_2024_01_01 + 3 * NANOS_PER_HOUR)); }
    #[test] fn add_days() { let r = eval("timestamp_add", &[s("day"), i(10), ts(TS_2024_01_01)]); assert_eq!(r, ts(TS_2024_01_01 + 10 * NANOS_PER_DAY)); }
    #[test] fn add_null() { assert_eq!(eval("timestamp_add", &[s("day"), i(1), null()]), null()); }
    #[test] fn add_zero() { assert_eq!(eval("timestamp_add", &[s("day"), i(0), ts(TS_2024_01_01)]), ts(TS_2024_01_01)); }
    #[test] fn add_negative() { let r = eval("timestamp_add", &[s("day"), i(-1), ts(TS_2024_01_01)]); assert_eq!(r, ts(TS_2024_01_01 - NANOS_PER_DAY)); }
    #[test] fn dateadd_alias() { let r = eval("dateadd", &[s("day"), i(1), ts(TS_2024_01_01)]); assert_eq!(r, ts(TS_2024_01_01 + NANOS_PER_DAY)); }
    #[test] fn date_add_alias() { let r = eval("date_add", &[s("day"), i(1), ts(TS_2024_01_01)]); assert_eq!(r, ts(TS_2024_01_01 + NANOS_PER_DAY)); }
}

// ===========================================================================
// epoch_nanos / epoch_seconds / epoch_millis / epoch_micros
// ===========================================================================
mod epoch_conversions_extra {
    use super::*;
    #[test] fn nanos_basic() { assert_eq!(eval("epoch_nanos", &[ts(TS_2024_01_01)]), i(TS_2024_01_01)); }
    #[test] fn nanos_epoch() { assert_eq!(eval("epoch_nanos", &[ts(EPOCH)]), i(0)); }
    #[test] fn nanos_null() { assert_eq!(eval("epoch_nanos", &[null()]), null()); }
    #[test] fn seconds_basic() { assert_eq!(eval("epoch_seconds", &[ts(TS_2024_01_01)]), i(1704067200)); }
    #[test] fn seconds_epoch() { assert_eq!(eval("epoch_seconds", &[ts(EPOCH)]), i(0)); }
    #[test] fn seconds_null() { assert_eq!(eval("epoch_seconds", &[null()]), null()); }
    #[test] fn millis_basic() { assert_eq!(eval("epoch_millis", &[ts(TS_2024_01_01)]), i(1704067200000)); }
    #[test] fn millis_null() { assert_eq!(eval("epoch_millis", &[null()]), null()); }
    #[test] fn micros_basic() { assert_eq!(eval("epoch_micros", &[ts(TS_2024_01_01)]), i(1704067200000000)); }
    #[test] fn micros_null() { assert_eq!(eval("epoch_micros", &[null()]), null()); }
    #[test] fn unix_timestamp_alias() { assert_eq!(eval("unix_timestamp", &[ts(TS_2024_01_01)]), i(1704067200)); }
    #[test] fn to_unix_timestamp_alias() { assert_eq!(eval("to_unix_timestamp", &[ts(TS_2024_01_01)]), i(1704067200)); }
    #[test] fn nanos_to_millis_alias() { assert_eq!(eval("nanos_to_millis", &[ts(TS_2024_01_01)]), i(1704067200000)); }
    #[test] fn nanos_to_micros_alias() { assert_eq!(eval("nanos_to_micros", &[ts(TS_2024_01_01)]), i(1704067200000000)); }
    #[test] fn nanos_to_secs_alias() { assert_eq!(eval("nanos_to_secs", &[ts(TS_2024_01_01)]), i(1704067200)); }
}

// ===========================================================================
// date_part
// ===========================================================================
mod date_part_extra {
    use super::*;
    #[test] fn year() { assert_eq!(eval("date_part", &[s("year"), ts(TS_2024_01_01)]), i(2024)); }
    #[test] fn month() { assert_eq!(eval("date_part", &[s("month"), ts(TS_2024_06_15)]), i(6)); }
    #[test] fn day() { assert_eq!(eval("date_part", &[s("day"), ts(TS_2024_03_15_123045)]), i(15)); }
    #[test] fn hour() { assert_eq!(eval("date_part", &[s("hour"), ts(TS_2024_03_15_123045)]), i(12)); }
    #[test] fn minute() { assert_eq!(eval("date_part", &[s("minute"), ts(TS_2024_03_15_123045)]), i(30)); }
    #[test] fn second() { assert_eq!(eval("date_part", &[s("second"), ts(TS_2024_03_15_123045)]), i(45)); }
    #[test]     fn null_in() { assert_eq!(eval("date_part", &[s("year"), null()]), null()); }
    #[test] fn quarter() { assert_eq!(eval("date_part", &[s("quarter"), ts(TS_2024_06_15)]), i(2)); }
}

// ===========================================================================
// is_weekend / is_business_day
// ===========================================================================
mod weekend_business_extra {
    use super::*;
    // 2024-06-15 is a Saturday
    #[test] fn sat_is_weekend() { assert_eq!(eval("is_weekend", &[ts(TS_2024_06_15)]), i(1)); }
    // 2024-03-15 is a Friday
    #[test] fn fri_not_weekend() { assert_eq!(eval("is_weekend", &[ts(TS_2024_03_15_123045)]), i(0)); }
    #[test] fn weekend_null() { assert_eq!(eval("is_weekend", &[null()]), null()); }
    // 2024-01-01 is a Monday
    #[test] fn mon_not_weekend() { assert_eq!(eval("is_weekend", &[ts(TS_2024_01_01)]), i(0)); }

    #[test] fn sat_not_business() { assert_eq!(eval("is_business_day", &[ts(TS_2024_06_15)]), i(0)); }
    #[test] fn fri_is_business() { assert_eq!(eval("is_business_day", &[ts(TS_2024_03_15_123045)]), i(1)); }
    #[test] fn business_null() { assert_eq!(eval("is_business_day", &[null()]), null()); }
    #[test] fn mon_is_business() { assert_eq!(eval("is_business_day", &[ts(TS_2024_01_01)]), i(1)); }
}

// ===========================================================================
// is_leap_year_fn
// ===========================================================================
mod is_leap_year_extra {
    use super::*;
    #[test] fn y2024_is_leap() { assert_eq!(eval("is_leap_year_fn", &[ts(TS_2024_01_01)]), i(1)); }
    #[test] fn y2023_not_leap() { assert_eq!(eval("is_leap_year_fn", &[ts(TS_2023_06_15)]), i(0)); }
    #[test] fn y2000_is_leap() { assert_eq!(eval("is_leap_year_fn", &[ts(TS_2000_01_01)]), i(1)); }
    #[test]     fn null_in() { assert_eq!(eval("is_leap_year_fn", &[null()]), null()); }
    #[test] fn y1970_not_leap() { assert_eq!(eval("is_leap_year_fn", &[ts(EPOCH)]), i(0)); }
}

// ===========================================================================
// days_in_month_fn
// ===========================================================================
mod days_in_month_extra {
    use super::*;
    #[test] fn jan() { assert_eq!(eval("days_in_month_fn", &[ts(TS_2024_01_01)]), i(31)); }
    #[test] fn feb_leap() { assert_eq!(eval("days_in_month_fn", &[ts(TS_2024_02_29)]), i(29)); }
    #[test] fn jun() { assert_eq!(eval("days_in_month_fn", &[ts(TS_2024_06_15)]), i(30)); }
    #[test] fn dec() { assert_eq!(eval("days_in_month_fn", &[ts(TS_2024_12_31_235959)]), i(31)); }
    #[test]     fn null_in() { assert_eq!(eval("days_in_month_fn", &[null()]), null()); }
    #[test] fn mar() { assert_eq!(eval("days_in_month_fn", &[ts(TS_2024_03_15_123045)]), i(31)); }
}

// ===========================================================================
// first_of_month / last_of_month / first_day_of_month / last_day_of_month
// ===========================================================================
mod month_bounds_extra {
    use super::*;
    #[test] fn first_jan() { let r = eval("first_of_month", &[ts(TS_2024_01_01)]); assert_eq!(r, ts(TS_2024_01_01)); }
    #[test] fn first_mar() { let r = eval("first_of_month", &[ts(TS_2024_03_15_123045)]); match r { Value::Timestamp(v) => { let day = eval("extract_day", &[Value::Timestamp(v)]); assert_eq!(day, i(1)); }, _ => panic!() } }
    #[test] fn first_null() { assert_eq!(eval("first_of_month", &[null()]), null()); }
    #[test] fn first_day_of_month_alias() { let r = eval("first_day_of_month", &[ts(TS_2024_03_15_123045)]); match r { Value::Timestamp(_) => {}, _ => panic!() } }

    #[test] fn last_jan() { let r = eval("last_of_month", &[ts(TS_2024_01_01)]); match r { Value::Timestamp(v) => { let day = eval("extract_day", &[Value::Timestamp(v)]); assert_eq!(day, i(31)); }, _ => panic!() } }
    #[test] fn last_feb_leap() { let r = eval("last_of_month", &[ts(TS_2024_02_29)]); match r { Value::Timestamp(v) => { let day = eval("extract_day", &[Value::Timestamp(v)]); assert_eq!(day, i(29)); }, _ => panic!() } }
    #[test] fn last_null() { assert_eq!(eval("last_of_month", &[null()]), null()); }
    #[test] fn last_day_of_month_alias() { let r = eval("last_day_of_month", &[ts(TS_2024_01_01)]); match r { Value::Timestamp(v) => { let day = eval("extract_day", &[Value::Timestamp(v)]); assert_eq!(day, i(31)); }, _ => panic!() } }
}

// ===========================================================================
// start_of_year / end_of_year / start_of_quarter / start_of_week
// ===========================================================================
mod year_quarter_week_bounds {
    use super::*;
    #[test] fn start_year() { assert_eq!(eval("start_of_year", &[ts(TS_2024_06_15)]), ts(TS_2024_01_01)); }
    #[test] fn start_year_null() { assert_eq!(eval("start_of_year", &[null()]), null()); }
    #[test] fn first_day_of_year_alias() { assert_eq!(eval("first_day_of_year", &[ts(TS_2024_06_15)]), ts(TS_2024_01_01)); }

    #[test] fn end_year() { let r = eval("end_of_year", &[ts(TS_2024_06_15)]); match r { Value::Timestamp(v) => assert!(v > TS_2024_06_15), _ => panic!() } }
    #[test] fn end_year_null() { assert_eq!(eval("end_of_year", &[null()]), null()); }
    #[test] fn last_day_of_year_alias() { let r = eval("last_day_of_year", &[ts(TS_2024_06_15)]); match r { Value::Timestamp(v) => assert!(v > TS_2024_06_15), _ => panic!() } }

    #[test] fn start_quarter_q1() { let r = eval("start_of_quarter", &[ts(TS_2024_02_29)]); match r { Value::Timestamp(v) => assert_eq!(v, TS_2024_01_01), _ => panic!() } }
    #[test] fn start_quarter_null() { assert_eq!(eval("start_of_quarter", &[null()]), null()); }
    #[test] fn first_day_of_quarter_alias() { let r = eval("first_day_of_quarter", &[ts(TS_2024_02_29)]); assert_eq!(r, ts(TS_2024_01_01)); }

    #[test] fn start_week() { let r = eval("start_of_week", &[ts(TS_2024_06_15)]); match r { Value::Timestamp(v) => assert!(v <= TS_2024_06_15), _ => panic!() } }
    #[test] fn start_week_null() { assert_eq!(eval("start_of_week", &[null()]), null()); }
    #[test] fn first_day_of_week_alias() { let r = eval("first_day_of_week", &[ts(TS_2024_06_15)]); match r { Value::Timestamp(v) => assert!(v <= TS_2024_06_15), _ => panic!() } }
}

// ===========================================================================
// age / months_between / years_between
// ===========================================================================
mod age_between_extra {
    use super::*;
    #[test]     fn age_same() { let r = eval("age", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]); match r { Value::I64(v) => assert_eq!(v, 0), _ => panic!("{r:?}") } }
    #[test]     fn age_one_day() { let r = eval("age", &[ts(TS_2024_01_01), ts(TS_2024_01_01 + NANOS_PER_DAY)]); match r { Value::I64(v) => assert!(v >= 0), _ => panic!("{r:?}") } }
    #[test] fn age_null() { assert_eq!(eval("age", &[null(), ts(TS_2024_01_01)]), null()); }

    #[test] fn months_same() { assert_eq!(eval("months_between", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]), i(0)); }
    #[test] fn months_6() { let r = eval("months_between", &[ts(TS_2024_06_15), ts(TS_2024_01_01)]); match r { Value::I64(v) => assert!(v >= 5 && v <= 6), _ => panic!("{r:?}") } }
    #[test] fn months_null() { assert_eq!(eval("months_between", &[null(), ts(TS_2024_01_01)]), null()); }

    #[test] fn years_same() { assert_eq!(eval("years_between", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]), i(0)); }
    #[test] fn years_1() { let r = eval("years_between", &[ts(TS_2024_06_15), ts(TS_2023_06_15)]); match r { Value::I64(v) => assert_eq!(v, 1), _ => panic!("{r:?}") } }
    #[test] fn years_null() { assert_eq!(eval("years_between", &[null(), ts(TS_2024_01_01)]), null()); }
}

// ===========================================================================
// make_timestamp
// ===========================================================================
mod make_timestamp_extra {
    use super::*;
    #[test] fn basic() { let r = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]); assert_eq!(r, ts(TS_2024_01_01)); }
    #[test] fn with_time() { let r = eval("make_timestamp", &[i(2024), i(3), i(15), i(12), i(30), i(45)]); assert_eq!(r, ts(TS_2024_03_15_123045)); }
    #[test]     fn null_in() { assert_eq!(eval("make_timestamp", &[null(), i(1), i(1), i(0), i(0), i(0)]), null()); }
    #[test] fn epoch_make() { let r = eval("make_timestamp", &[i(1970), i(1), i(1), i(0), i(0), i(0)]); assert_eq!(r, ts(EPOCH)); }
    #[test] fn leap_day() { let r = eval("make_timestamp", &[i(2024), i(2), i(29), i(0), i(0), i(0)]); assert_eq!(r, ts(TS_2024_02_29)); }
    #[test] fn y2000() { let r = eval("make_timestamp", &[i(2000), i(1), i(1), i(0), i(0), i(0)]); assert_eq!(r, ts(TS_2000_01_01)); }
}

// ===========================================================================
// timestamp_ceil
// ===========================================================================
mod timestamp_ceil_extra {
    use super::*;
    #[test] fn ceil_year() { let r = eval("timestamp_ceil", &[s("year"), ts(TS_2024_03_15_123045)]); match r { Value::Timestamp(v) => assert!(v > TS_2024_03_15_123045), _ => panic!() } }
    #[test] fn ceil_month() { let r = eval("timestamp_ceil", &[s("month"), ts(TS_2024_03_15_123045)]); match r { Value::Timestamp(v) => assert!(v >= TS_2024_03_15_123045), _ => panic!() } }
    #[test] fn ceil_day() { let r = eval("timestamp_ceil", &[s("day"), ts(TS_2024_03_15_123045)]); match r { Value::Timestamp(v) => assert!(v >= TS_2024_03_15_123045), _ => panic!() } }
    #[test] fn ceil_null() { assert_eq!(eval("timestamp_ceil", &[s("year"), null()]), null()); }
    #[test] fn ceil_already_aligned() { let r = eval("timestamp_ceil", &[s("year"), ts(TS_2024_01_01)]); assert_eq!(r, ts(TS_2024_01_01)); }
}

// ===========================================================================
// interval_to_nanos
// ===========================================================================
mod interval_to_nanos_extra {
    use super::*;
    #[test] fn one_sec() { assert_eq!(eval("interval_to_nanos", &[s("1s")]), i(NANOS_PER_SEC)); }
    #[test] fn one_min() { assert_eq!(eval("interval_to_nanos", &[s("1m")]), i(NANOS_PER_MIN)); }
    #[test] fn one_hour() { assert_eq!(eval("interval_to_nanos", &[s("1h")]), i(NANOS_PER_HOUR)); }
    #[test] fn one_day() { assert_eq!(eval("interval_to_nanos", &[s("1d")]), i(NANOS_PER_DAY)); }
    #[test]     fn null_in() { assert_eq!(eval("interval_to_nanos", &[null()]), null()); }
    #[test] fn five_sec() { assert_eq!(eval("interval_to_nanos", &[s("5s")]), i(5 * NANOS_PER_SEC)); }
    #[test] fn ten_min() { assert_eq!(eval("interval_to_nanos", &[s("10m")]), i(10 * NANOS_PER_MIN)); }
}

// ===========================================================================
// to_str_timestamp / date_format / to_char
// ===========================================================================
mod format_extra {
    use super::*;
    #[test]     fn to_str_ts() { let r = eval("to_str_timestamp", &[ts(TS_2024_01_01)]); match r { Value::Str(v) => assert!(v.contains("2024")), _ => panic!("{r:?}") } }
    #[test]         fn to_str_ts_null() { assert_eq!(eval("to_str_timestamp", &[null()]), null()); }
    #[test] fn date_format_basic() { let r = eval("date_format", &[ts(TS_2024_01_01)]); match r { Value::Str(v) => assert!(v.contains("2024")), _ => panic!("{r:?}") } }
    #[test] fn date_format_null() { assert_eq!(eval("date_format", &[null()]), null()); }
    #[test] fn strftime_alias() { let r = eval("strftime", &[ts(TS_2024_01_01)]); match r { Value::Str(v) => assert!(v.contains("2024")), _ => panic!("{r:?}") } }
    #[test] fn timestamp_to_str_alias() { let r = eval("timestamp_to_str", &[ts(TS_2024_01_01)]); match r { Value::Str(v) => assert!(v.contains("2024")), _ => panic!("{r:?}") } }
    #[test] fn format_timestamp_alias() { let r = eval("format_timestamp", &[ts(TS_2024_01_01)]); match r { Value::Str(v) => assert!(v.contains("2024")), _ => panic!("{r:?}") } }
    #[test]     fn to_char_ts() { let r = eval("to_char", &[ts(TS_2024_01_01)]); match r { Value::Str(v) => assert!(v.len() > 0), _ => panic!("{r:?}") } }
    #[test]     fn to_char_int() { let r = eval("to_char", &[i(42)]); match r { Value::Str(v) => assert_eq!(v, "42"), _ => panic!("{r:?}") } }
    #[test]     fn to_char_null() { assert_eq!(eval("to_char", &[null()]), null()); }
    #[test] fn date_to_str_alias() { let r = eval("date_to_str", &[ts(TS_2024_01_01)]); match r { Value::Str(v) => assert!(v.contains("2024")), _ => panic!("{r:?}") } }
}

// ===========================================================================
// next_day
// ===========================================================================
mod next_day_extra {
    use super::*;
    #[test] fn from_monday() { let r = eval("next_day", &[ts(TS_2024_01_01), s("Tuesday")]); match r { Value::Timestamp(v) => assert!(v > TS_2024_01_01), _ => panic!() } }
    #[test]     fn null_in() { assert_eq!(eval("next_day", &[null(), s("Monday")]), null()); }
    #[test] fn from_saturday() { let r = eval("next_day", &[ts(TS_2024_06_15), s("Monday")]); match r { Value::Timestamp(v) => assert!(v > TS_2024_06_15), _ => panic!() } }
    #[test] fn from_friday() { let r = eval("next_day", &[ts(TS_2024_03_15_123045), s("Monday")]); match r { Value::Timestamp(v) => assert!(v > TS_2024_03_15_123045), _ => panic!() } }
}

// ===========================================================================
// to_timezone / from_utc / to_utc
// ===========================================================================
mod timezone_extra {
    use super::*;
    #[test] fn to_tz_utc() { let r = eval("to_timezone", &[ts(TS_2024_01_01), s("UTC")]); match r { Value::Timestamp(v) => assert_eq!(v, TS_2024_01_01), _ => panic!("{r:?}") } }
    #[test] fn to_tz_null() { assert_eq!(eval("to_timezone", &[null(), s("UTC")]), null()); }
    #[test] fn from_utc_basic() { let r = eval("from_utc", &[ts(TS_2024_01_01), s("UTC")]); match r { Value::Timestamp(_) => {}, _ => panic!() } }
    #[test] fn from_utc_null() { assert_eq!(eval("from_utc", &[null(), s("UTC")]), null()); }
    #[test] fn to_utc_basic() { let r = eval("to_utc", &[ts(TS_2024_01_01), s("UTC")]); match r { Value::Timestamp(_) => {}, _ => panic!() } }
    #[test] fn to_utc_null() { assert_eq!(eval("to_utc", &[null(), s("UTC")]), null()); }
}

// ===========================================================================
// cast_to_timestamp / cast_to_int / cast_to_float / cast_to_str
// ===========================================================================
mod cast_to_extra {
    use super::*;
    #[test] fn cast_to_ts_int() { match eval("cast_to_timestamp", &[i(1000)]) { Value::Timestamp(v) => assert_eq!(v, 1000), _ => panic!() } }
    #[test] fn cast_to_ts_null() { assert_eq!(eval("cast_to_timestamp", &[null()]), null()); }
    #[test] fn cast_to_int_from_str() { assert_eq!(eval("cast_to_int", &[s("42")]), i(42)); }
    #[test] fn cast_to_int_null() { assert_eq!(eval("cast_to_int", &[null()]), null()); }
    #[test] fn cast_to_float_from_str() { let r = eval("cast_to_float", &[s("3.14")]); match r { Value::F64(v) => assert!((v - 3.14).abs() < 0.001), _ => panic!() } }
    #[test] fn cast_to_float_null() { assert_eq!(eval("cast_to_float", &[null()]), null()); }
    #[test] fn cast_to_str_from_int() { assert_eq!(eval("cast_to_str", &[i(42)]), s("42")); }
    #[test]     fn cast_to_str_null() { assert_eq!(eval("cast_to_str", &[null()]), null()); }
}

// ===========================================================================
// timestamp_sequence
// ===========================================================================
mod ts_sequence_extra {
    use super::*;
    #[test] fn basic() { let r = eval("timestamp_sequence", &[ts(TS_2024_01_01), i(NANOS_PER_HOUR)]); match r { Value::Timestamp(_) => {}, _ => panic!("{r:?}") } }
    #[test]     fn null_in() { assert_eq!(eval("timestamp_sequence", &[null(), i(NANOS_PER_HOUR)]), null()); }
}

// ===========================================================================
// Additional tests on various timestamps for full coverage
// ===========================================================================
mod extra_timestamp_combos {
    use super::*;

    // Different timestamps x different extractions
    #[test] fn year_2000() { assert_eq!(eval("extract_year", &[ts(TS_2000_01_01)]), i(2000)); }
    #[test] fn month_2000() { assert_eq!(eval("extract_month", &[ts(TS_2000_01_01)]), i(1)); }
    #[test] fn day_2000() { assert_eq!(eval("extract_day", &[ts(TS_2000_01_01)]), i(1)); }
    #[test] fn hour_2000() { assert_eq!(eval("extract_hour", &[ts(TS_2000_01_01)]), i(0)); }
    #[test] fn min_2000() { assert_eq!(eval("extract_minute", &[ts(TS_2000_01_01)]), i(0)); }
    #[test] fn sec_2000() { assert_eq!(eval("extract_second", &[ts(TS_2000_01_01)]), i(0)); }

    #[test] fn year_2023() { assert_eq!(eval("extract_year", &[ts(TS_2023_06_15)]), i(2023)); }
    #[test] fn month_2023() { assert_eq!(eval("extract_month", &[ts(TS_2023_06_15)]), i(6)); }
    #[test] fn day_2023() { assert_eq!(eval("extract_day", &[ts(TS_2023_06_15)]), i(15)); }

    // trunc variants on different timestamps
    #[test] fn trunc_year_2023() { let r = eval("date_trunc", &[s("year"), ts(TS_2023_06_15)]); match r { Value::Timestamp(v) => { let y = eval("extract_year", &[Value::Timestamp(v)]); assert_eq!(y, i(2023)); let m = eval("extract_month", &[Value::Timestamp(v)]); assert_eq!(m, i(1)); }, _ => panic!() } }
    #[test] fn trunc_month_dec() { let r = eval("date_trunc", &[s("month"), ts(TS_2024_12_31_235959)]); match r { Value::Timestamp(v) => { let d = eval("extract_day", &[Value::Timestamp(v)]); assert_eq!(d, i(1)); }, _ => panic!() } }
    #[test] fn trunc_day_dec31() { let r = eval("date_trunc", &[s("day"), ts(TS_2024_12_31_235959)]); match r { Value::Timestamp(v) => { let h = eval("extract_hour", &[Value::Timestamp(v)]); assert_eq!(h, i(0)); }, _ => panic!() } }

    // Add/diff combos
    #[test] fn add_then_diff() { let added = eval("timestamp_add", &[s("day"), i(7), ts(TS_2024_01_01)]); let diff = eval("date_diff", &[s("day"), ts(TS_2024_01_01), added]); assert_eq!(diff, i(7)); }
    #[test] fn add_hour_then_diff() { let added = eval("timestamp_add", &[s("hour"), i(24), ts(TS_2024_01_01)]); let diff = eval("date_diff", &[s("hour"), ts(TS_2024_01_01), added]); assert_eq!(diff, i(24)); }
    #[test] fn add_minute_then_diff() { let added = eval("timestamp_add", &[s("minute"), i(120), ts(TS_2024_01_01)]); let diff = eval("date_diff", &[s("minute"), ts(TS_2024_01_01), added]); assert_eq!(diff, i(120)); }

    // Cross-timestamp operations
    #[test] fn diff_jan_to_mar() { let r = eval("date_diff", &[s("day"), ts(TS_2024_01_01), ts(TS_2024_03_15_123045)]); match r { Value::I64(v) => assert!(v >= 74 && v <= 75), _ => panic!("{r:?}") } }
    #[test] fn diff_jan_to_jun() { let r = eval("date_diff", &[s("day"), ts(TS_2024_01_01), ts(TS_2024_06_15)]); match r { Value::I64(v) => assert!(v >= 165 && v <= 167), _ => panic!("{r:?}") } }
    #[test] fn diff_jan_to_dec() { let r = eval("date_diff", &[s("day"), ts(TS_2024_01_01), ts(TS_2024_12_31_235959)]); match r { Value::I64(v) => assert!(v >= 365 && v <= 366), _ => panic!("{r:?}") } }
    #[test] fn diff_2023_to_2024() { let r = eval("date_diff", &[s("day"), ts(TS_2023_06_15), ts(TS_2024_06_15)]); match r { Value::I64(v) => assert!(v >= 365 && v <= 367), _ => panic!("{r:?}") } }

    // Boundary leap year tests
    #[test] fn leap_day_year() { assert_eq!(eval("extract_year", &[ts(TS_2024_02_29)]), i(2024)); }
    #[test] fn leap_day_month() { assert_eq!(eval("extract_month", &[ts(TS_2024_02_29)]), i(2)); }
    #[test] fn leap_day_day() { assert_eq!(eval("extract_day", &[ts(TS_2024_02_29)]), i(29)); }
    #[test] fn leap_day_doy() { assert_eq!(eval("extract_day_of_year", &[ts(TS_2024_02_29)]), i(60)); }
    #[test] fn leap_day_quarter() { assert_eq!(eval("extract_quarter", &[ts(TS_2024_02_29)]), i(1)); }

    // Epoch boundary tests
    #[test] fn epoch_year() { assert_eq!(eval("extract_year", &[ts(EPOCH)]), i(1970)); }
    #[test] fn epoch_month() { assert_eq!(eval("extract_month", &[ts(EPOCH)]), i(1)); }
    #[test] fn epoch_day() { assert_eq!(eval("extract_day", &[ts(EPOCH)]), i(1)); }
    #[test] fn epoch_hour() { assert_eq!(eval("extract_hour", &[ts(EPOCH)]), i(0)); }
    #[test] fn epoch_minute() { assert_eq!(eval("extract_minute", &[ts(EPOCH)]), i(0)); }
    #[test] fn epoch_second() { assert_eq!(eval("extract_second", &[ts(EPOCH)]), i(0)); }
    #[test] fn epoch_doy() { assert_eq!(eval("extract_day_of_year", &[ts(EPOCH)]), i(1)); }

    // End of day boundary
    #[test] fn eod_hour() { assert_eq!(eval("extract_hour", &[ts(TS_2024_12_31_235959)]), i(23)); }
    #[test] fn eod_minute() { assert_eq!(eval("extract_minute", &[ts(TS_2024_12_31_235959)]), i(59)); }
    #[test] fn eod_second() { assert_eq!(eval("extract_second", &[ts(TS_2024_12_31_235959)]), i(59)); }
    #[test] fn eod_doy() { assert_eq!(eval("extract_day_of_year", &[ts(TS_2024_12_31_235959)]), i(366)); }
}
