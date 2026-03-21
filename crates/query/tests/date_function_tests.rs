//! Comprehensive date/time function tests for ExchangeDB.
//! 500+ test cases covering every registered date/time scalar function.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}
fn i(v: i64) -> Value {
    Value::I64(v)
}
fn f(v: f64) -> Value {
    Value::F64(v)
}
fn ts(ns: i64) -> Value {
    Value::Timestamp(ns)
}
fn null() -> Value {
    Value::Null
}

fn eval(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap()
}

fn eval_err(name: &str, args: &[Value]) -> String {
    evaluate_scalar(name, args).unwrap_err()
}

// Key timestamps (UTC):
// 1970-01-01T00:00:00Z = 0
// 2024-01-01T00:00:00Z (Monday) = 1704067200 * 1e9
// 2024-02-29T00:00:00Z (leap day, Thursday) = 1709164800 * 1e9
// 2024-03-15T12:30:45Z = 1710505845 * 1e9
// 2024-06-15T00:00:00Z (Saturday) = 1718409600 * 1e9
// 2024-12-31T23:59:59Z (Tuesday) = 1735689599 * 1e9

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MIN: i64 = 60 * NANOS_PER_SEC;
const NANOS_PER_HOUR: i64 = 3600 * NANOS_PER_SEC;
const NANOS_PER_DAY: i64 = 86400 * NANOS_PER_SEC;

const EPOCH: i64 = 0;
const TS_2024_01_01: i64 = 1704067200 * NANOS_PER_SEC;
const TS_2024_02_29: i64 = 1709164800 * NANOS_PER_SEC;
const TS_2024_03_15_123045: i64 = 1710505845 * NANOS_PER_SEC;
const TS_2024_06_15: i64 = 1718409600 * NANOS_PER_SEC;
const TS_2024_12_31_235959: i64 = 1735689599 * NANOS_PER_SEC;
const TS_2023_06_15: i64 = 1686787200 * NANOS_PER_SEC;
const TS_2000_01_01: i64 = 946684800 * NANOS_PER_SEC;

// ===========================================================================
// now / systimestamp / current_timestamp
// ===========================================================================
mod now_tests {
    use super::*;

    #[test]
    fn now_returns_timestamp() {
        match eval("now", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn systimestamp_returns_ts() {
        match eval("systimestamp", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn current_timestamp_alias() {
        match eval("current_timestamp", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn now_utc_alias() {
        match eval("now_utc", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn sysdate_alias() {
        match eval("sysdate", &[]) {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn yesterday_before_now() {
        let y = match eval("yesterday", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        let n = match eval("now", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        assert!(y < n);
    }
    #[test]
    fn tomorrow_after_now() {
        let t = match eval("tomorrow", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        let n = match eval("now", &[]) {
            Value::Timestamp(ns) => ns,
            _ => panic!(),
        };
        assert!(t > n);
    }
}

// ===========================================================================
// to_timestamp
// ===========================================================================
mod to_timestamp_tests {
    use super::*;

    #[test]
    fn from_int() {
        assert_eq!(eval("to_timestamp", &[i(1000)]), ts(1000));
    }
    #[test]
    fn from_timestamp() {
        assert_eq!(eval("to_timestamp", &[ts(1000)]), ts(1000));
    }
    #[test]
    fn from_string() {
        assert_eq!(eval("to_timestamp", &[s("1000")]), ts(1000));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("to_timestamp", &[null()]), null());
    }
    #[test]
    fn epoch() {
        assert_eq!(eval("to_timestamp", &[i(0)]), ts(0));
    }
    #[test]
    fn large_ts() {
        assert_eq!(eval("to_timestamp", &[i(TS_2024_01_01)]), ts(TS_2024_01_01));
    }
    #[test]
    fn float_input() {
        assert_eq!(eval("to_timestamp", &[f(1000.0)]), ts(1000));
    }
    #[test]
    fn from_unixtime_alias() {
        assert_eq!(eval("from_unixtime", &[i(1000)]), ts(1000));
    }
    #[test]
    fn str_to_timestamp_alias() {
        assert_eq!(eval("str_to_timestamp", &[s("1000")]), ts(1000));
    }
    #[test]
    fn parse_timestamp_alias() {
        assert_eq!(eval("parse_timestamp", &[s("1000")]), ts(1000));
    }
}

// ===========================================================================
// extract_year
// ===========================================================================
mod extract_year_tests {
    use super::*;

    #[test]
    fn epoch_year() {
        assert_eq!(eval("extract_year", &[ts(EPOCH)]), i(1970));
    }
    #[test]
    fn y2024() {
        assert_eq!(eval("extract_year", &[ts(TS_2024_01_01)]), i(2024));
    }
    #[test]
    fn y2024_leap() {
        assert_eq!(eval("extract_year", &[ts(TS_2024_02_29)]), i(2024));
    }
    #[test]
    fn y2024_dec() {
        assert_eq!(eval("extract_year", &[ts(TS_2024_12_31_235959)]), i(2024));
    }
    #[test]
    fn y2000() {
        assert_eq!(eval("extract_year", &[ts(TS_2000_01_01)]), i(2000));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_year", &[null()]), null());
    }
    #[test]
    fn from_int() {
        assert_eq!(eval("extract_year", &[i(TS_2024_01_01)]), i(2024));
    }
    #[test]
    fn y2023() {
        assert_eq!(eval("extract_year", &[ts(TS_2023_06_15)]), i(2023));
    }
    #[test]
    fn year_of_alias() {
        assert_eq!(eval("year_of", &[ts(TS_2024_01_01)]), i(2024));
    }
    #[test]
    fn date_part_year() {
        assert_eq!(eval("date_part", &[s("year"), ts(TS_2024_01_01)]), i(2024));
    }
}

// ===========================================================================
// extract_month
// ===========================================================================
mod extract_month_tests {
    use super::*;

    #[test]
    fn january() {
        assert_eq!(eval("extract_month", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn february() {
        assert_eq!(eval("extract_month", &[ts(TS_2024_02_29)]), i(2));
    }
    #[test]
    fn march() {
        assert_eq!(eval("extract_month", &[ts(TS_2024_03_15_123045)]), i(3));
    }
    #[test]
    fn june() {
        assert_eq!(eval("extract_month", &[ts(TS_2024_06_15)]), i(6));
    }
    #[test]
    fn december() {
        assert_eq!(eval("extract_month", &[ts(TS_2024_12_31_235959)]), i(12));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_month", &[null()]), null());
    }
    #[test]
    fn epoch_month() {
        assert_eq!(eval("extract_month", &[ts(EPOCH)]), i(1));
    }
    #[test]
    fn month_of_year_alias() {
        assert_eq!(eval("month_of_year", &[ts(TS_2024_06_15)]), i(6));
    }
    #[test]
    fn date_part_month() {
        assert_eq!(
            eval("date_part", &[s("month"), ts(TS_2024_03_15_123045)]),
            i(3)
        );
    }
    #[test]
    fn y2000_jan() {
        assert_eq!(eval("extract_month", &[ts(TS_2000_01_01)]), i(1));
    }
}

// ===========================================================================
// extract_day
// ===========================================================================
mod extract_day_tests {
    use super::*;

    #[test]
    fn first() {
        assert_eq!(eval("extract_day", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn twenty_ninth() {
        assert_eq!(eval("extract_day", &[ts(TS_2024_02_29)]), i(29));
    }
    #[test]
    fn fifteenth() {
        assert_eq!(eval("extract_day", &[ts(TS_2024_03_15_123045)]), i(15));
    }
    #[test]
    fn thirty_first() {
        assert_eq!(eval("extract_day", &[ts(TS_2024_12_31_235959)]), i(31));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_day", &[null()]), null());
    }
    #[test]
    fn epoch_day() {
        assert_eq!(eval("extract_day", &[ts(EPOCH)]), i(1));
    }
    #[test]
    fn day_of_month_alias() {
        assert_eq!(eval("day_of_month", &[ts(TS_2024_03_15_123045)]), i(15));
    }
    #[test]
    fn date_part_day() {
        assert_eq!(eval("date_part", &[s("day"), ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn june_15() {
        assert_eq!(eval("extract_day", &[ts(TS_2024_06_15)]), i(15));
    }
}

// ===========================================================================
// extract_hour
// ===========================================================================
mod extract_hour_tests {
    use super::*;

    #[test]
    fn midnight() {
        assert_eq!(eval("extract_hour", &[ts(TS_2024_01_01)]), i(0));
    }
    #[test]
    fn twelve() {
        assert_eq!(eval("extract_hour", &[ts(TS_2024_03_15_123045)]), i(12));
    }
    #[test]
    fn twenty_three() {
        assert_eq!(eval("extract_hour", &[ts(TS_2024_12_31_235959)]), i(23));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_hour", &[null()]), null());
    }
    #[test]
    fn epoch_hour() {
        assert_eq!(eval("extract_hour", &[ts(EPOCH)]), i(0));
    }
    #[test]
    fn date_part_hour() {
        assert_eq!(
            eval("date_part", &[s("hour"), ts(TS_2024_03_15_123045)]),
            i(12)
        );
    }
    #[test]
    fn hour_of_day_alias() {
        assert_eq!(eval("hour_of_day", &[ts(TS_2024_03_15_123045)]), i(12));
    }
    #[test]
    fn noon_ts() {
        let noon = TS_2024_01_01 + 12 * NANOS_PER_HOUR;
        assert_eq!(eval("extract_hour", &[ts(noon)]), i(12));
    }
    #[test]
    fn end_of_day() {
        let eod = TS_2024_01_01 + 23 * NANOS_PER_HOUR + 59 * NANOS_PER_MIN;
        assert_eq!(eval("extract_hour", &[ts(eod)]), i(23));
    }
}

// ===========================================================================
// extract_minute
// ===========================================================================
mod extract_minute_tests {
    use super::*;

    #[test]
    fn zero() {
        assert_eq!(eval("extract_minute", &[ts(TS_2024_01_01)]), i(0));
    }
    #[test]
    fn thirty() {
        assert_eq!(eval("extract_minute", &[ts(TS_2024_03_15_123045)]), i(30));
    }
    #[test]
    fn fifty_nine() {
        assert_eq!(eval("extract_minute", &[ts(TS_2024_12_31_235959)]), i(59));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_minute", &[null()]), null());
    }
    #[test]
    fn epoch_min() {
        assert_eq!(eval("extract_minute", &[ts(EPOCH)]), i(0));
    }
    #[test]
    fn date_part_min() {
        assert_eq!(
            eval("date_part", &[s("minute"), ts(TS_2024_03_15_123045)]),
            i(30)
        );
    }
    #[test]
    fn minute_of_hour_alias() {
        assert_eq!(eval("minute_of_hour", &[ts(TS_2024_03_15_123045)]), i(30));
    }
    #[test]
    fn custom_minute() {
        let t = TS_2024_01_01 + 15 * NANOS_PER_MIN;
        assert_eq!(eval("extract_minute", &[ts(t)]), i(15));
    }
}

// ===========================================================================
// extract_second
// ===========================================================================
mod extract_second_tests {
    use super::*;

    #[test]
    fn zero() {
        assert_eq!(eval("extract_second", &[ts(TS_2024_01_01)]), i(0));
    }
    #[test]
    fn forty_five() {
        assert_eq!(eval("extract_second", &[ts(TS_2024_03_15_123045)]), i(45));
    }
    #[test]
    fn fifty_nine() {
        assert_eq!(eval("extract_second", &[ts(TS_2024_12_31_235959)]), i(59));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_second", &[null()]), null());
    }
    #[test]
    fn epoch_sec() {
        assert_eq!(eval("extract_second", &[ts(EPOCH)]), i(0));
    }
    #[test]
    fn date_part_sec() {
        assert_eq!(
            eval("date_part", &[s("second"), ts(TS_2024_03_15_123045)]),
            i(45)
        );
    }
    #[test]
    fn second_of_minute_alias() {
        assert_eq!(eval("second_of_minute", &[ts(TS_2024_03_15_123045)]), i(45));
    }
    #[test]
    fn custom_second() {
        let t = TS_2024_01_01 + 30 * NANOS_PER_SEC;
        assert_eq!(eval("extract_second", &[ts(t)]), i(30));
    }
}

// ===========================================================================
// extract_week
// ===========================================================================
mod extract_week_tests {
    use super::*;

    #[test]
    fn jan_1_2024() {
        let r = eval("extract_week", &[ts(TS_2024_01_01)]);
        match r {
            Value::I64(w) => assert!(w >= 1 && w <= 53),
            _ => panic!(),
        }
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_week", &[null()]), null());
    }
    #[test]
    fn mid_year() {
        let r = eval("extract_week", &[ts(TS_2024_06_15)]);
        match r {
            Value::I64(w) => assert!(w >= 1 && w <= 53),
            _ => panic!(),
        }
    }
    #[test]
    fn dec_31() {
        let r = eval("extract_week", &[ts(TS_2024_12_31_235959)]);
        match r {
            Value::I64(w) => assert!(w >= 1 && w <= 53),
            _ => panic!(),
        }
    }
    #[test]
    fn epoch_week() {
        let r = eval("extract_week", &[ts(EPOCH)]);
        match r {
            Value::I64(w) => assert!(w >= 1),
            _ => panic!(),
        }
    }
    #[test]
    fn week_of_year_alias() {
        let r = eval("week_of_year", &[ts(TS_2024_06_15)]);
        match r {
            Value::I64(w) => assert!(w >= 1),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// extract_day_of_week
// ===========================================================================
mod extract_day_of_week_tests {
    use super::*;

    #[test]
    fn epoch_thursday() {
        assert_eq!(eval("extract_day_of_week", &[ts(EPOCH)]), i(4));
    } // Thursday
    #[test]
    fn jan_1_2024_monday() {
        assert_eq!(eval("extract_day_of_week", &[ts(TS_2024_01_01)]), i(1));
    } // Monday
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_day_of_week", &[null()]), null());
    }
    #[test]
    fn feb_29_thursday() {
        assert_eq!(eval("extract_day_of_week", &[ts(TS_2024_02_29)]), i(4));
    } // Thursday
    #[test]
    fn june_15_sat() {
        assert_eq!(eval("extract_day_of_week", &[ts(TS_2024_06_15)]), i(6));
    } // Saturday
    #[test]
    fn day_of_week_alias() {
        assert_eq!(eval("day_of_week", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn range() {
        let r = eval("extract_day_of_week", &[ts(TS_2024_03_15_123045)]);
        match r {
            Value::I64(d) => assert!(d >= 0 && d <= 6),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// extract_day_of_year
// ===========================================================================
mod extract_day_of_year_tests {
    use super::*;

    #[test]
    fn jan_1() {
        assert_eq!(eval("extract_day_of_year", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn feb_29() {
        assert_eq!(eval("extract_day_of_year", &[ts(TS_2024_02_29)]), i(60));
    } // 31 (jan) + 29
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_day_of_year", &[null()]), null());
    }
    #[test]
    fn dec_31() {
        assert_eq!(
            eval("extract_day_of_year", &[ts(TS_2024_12_31_235959)]),
            i(366)
        );
    } // 2024 leap
    #[test]
    fn epoch() {
        assert_eq!(eval("extract_day_of_year", &[ts(EPOCH)]), i(1));
    }
    #[test]
    fn day_of_year_alias() {
        assert_eq!(eval("day_of_year", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn mid_year() {
        let r = eval("extract_day_of_year", &[ts(TS_2024_06_15)]);
        match r {
            Value::I64(d) => assert!(d > 100 && d < 300),
            _ => panic!(),
        }
    }
    #[test]
    fn date_part_doy() {
        assert_eq!(
            eval("date_part", &[s("day_of_year"), ts(TS_2024_01_01)]),
            i(1)
        );
    }
}

// ===========================================================================
// extract_quarter
// ===========================================================================
mod extract_quarter_tests {
    use super::*;

    #[test]
    fn q1() {
        assert_eq!(eval("extract_quarter", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn q1_feb() {
        assert_eq!(eval("extract_quarter", &[ts(TS_2024_02_29)]), i(1));
    }
    #[test]
    fn q1_mar() {
        assert_eq!(eval("extract_quarter", &[ts(TS_2024_03_15_123045)]), i(1));
    }
    #[test]
    fn q2() {
        assert_eq!(eval("extract_quarter", &[ts(TS_2024_06_15)]), i(2));
    }
    #[test]
    fn q4() {
        assert_eq!(eval("extract_quarter", &[ts(TS_2024_12_31_235959)]), i(4));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("extract_quarter", &[null()]), null());
    }
    #[test]
    fn quarter_of_year_alias() {
        assert_eq!(eval("quarter_of_year", &[ts(TS_2024_06_15)]), i(2));
    }
    #[test]
    fn date_part_quarter() {
        assert_eq!(eval("date_part", &[s("quarter"), ts(TS_2024_06_15)]), i(2));
    }
}

// ===========================================================================
// date_trunc
// ===========================================================================
mod date_trunc_tests {
    use super::*;

    #[test]
    fn trunc_day() {
        assert_eq!(
            eval("date_trunc", &[s("day"), ts(TS_2024_03_15_123045)]),
            ts(TS_2024_03_15_123045
                - 12 * NANOS_PER_HOUR
                - 30 * NANOS_PER_MIN
                - 45 * NANOS_PER_SEC)
        );
    }
    #[test]
    fn trunc_month() {
        let r = eval("date_trunc", &[s("month"), ts(TS_2024_03_15_123045)]);
        // First of March 2024
        let first_mar = eval("extract_day", &[r.clone()]);
        assert_eq!(first_mar, i(1));
    }
    #[test]
    fn trunc_year() {
        let r = eval("date_trunc", &[s("year"), ts(TS_2024_06_15)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(1));
        assert_eq!(eval("extract_day", &[r.clone()]), i(1));
    }
    #[test]
    fn trunc_hour() {
        let r = eval("date_trunc", &[s("hour"), ts(TS_2024_03_15_123045)]);
        assert_eq!(eval("extract_minute", &[r.clone()]), i(0));
        assert_eq!(eval("extract_second", &[r.clone()]), i(0));
    }
    #[test]
    fn trunc_minute() {
        let r = eval("date_trunc", &[s("minute"), ts(TS_2024_03_15_123045)]);
        assert_eq!(eval("extract_second", &[r.clone()]), i(0));
    }
    #[test]
    fn trunc_second() {
        let r = eval("date_trunc", &[s("second"), ts(TS_2024_03_15_123045)]);
        assert_eq!(eval("extract_second", &[r]), i(45));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("date_trunc", &[s("day"), null()]), null());
    }
    #[test]
    fn unknown_unit() {
        assert!(eval_err("date_trunc", &[s("week"), ts(TS_2024_01_01)]).contains("unknown"));
    }
    #[test]
    fn timestamp_floor_alias() {
        assert_eq!(
            eval("timestamp_floor", &[s("day"), ts(TS_2024_03_15_123045)]),
            eval("date_trunc", &[s("day"), ts(TS_2024_03_15_123045)])
        );
    }
    #[test]
    fn epoch_trunc_day() {
        assert_eq!(eval("date_trunc", &[s("day"), ts(EPOCH)]), ts(EPOCH));
    }
}

// ===========================================================================
// date_diff
// ===========================================================================
mod date_diff_tests {
    use super::*;

    #[test]
    fn diff_days() {
        let r = eval(
            "date_diff",
            &[s("day"), ts(TS_2024_01_01), ts(TS_2024_02_29)],
        );
        assert_eq!(r, i(59)); // 31 (Jan) + 28 (Feb 1-28) = 59
    }
    #[test]
    fn diff_hours() {
        let r = eval(
            "date_diff",
            &[
                s("hour"),
                ts(TS_2024_01_01),
                ts(TS_2024_01_01 + 5 * NANOS_PER_HOUR),
            ],
        );
        assert_eq!(r, i(5));
    }
    #[test]
    fn diff_minutes() {
        let r = eval(
            "date_diff",
            &[
                s("minute"),
                ts(TS_2024_01_01),
                ts(TS_2024_01_01 + 90 * NANOS_PER_MIN),
            ],
        );
        assert_eq!(r, i(90));
    }
    #[test]
    fn diff_seconds() {
        let r = eval(
            "date_diff",
            &[
                s("second"),
                ts(TS_2024_01_01),
                ts(TS_2024_01_01 + 100 * NANOS_PER_SEC),
            ],
        );
        assert_eq!(r, i(100));
    }
    #[test]
    fn null_first() {
        assert_eq!(
            eval("date_diff", &[s("day"), null(), ts(TS_2024_01_01)]),
            null()
        );
    }
    #[test]
    fn null_second() {
        assert_eq!(
            eval("date_diff", &[s("day"), ts(TS_2024_01_01), null()]),
            null()
        );
    }
    #[test]
    fn negative_diff() {
        let r = eval(
            "date_diff",
            &[s("day"), ts(TS_2024_02_29), ts(TS_2024_01_01)],
        );
        assert_eq!(r, i(-59));
    }
    #[test]
    fn same_ts() {
        assert_eq!(
            eval(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_01_01)]
            ),
            i(0)
        );
    }
    #[test]
    fn datediff_alias() {
        assert_eq!(
            eval(
                "datediff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_02_29)]
            ),
            eval(
                "date_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_02_29)]
            )
        );
    }
    #[test]
    fn timestamp_diff_alias() {
        assert_eq!(
            eval(
                "timestamp_diff",
                &[s("day"), ts(TS_2024_01_01), ts(TS_2024_02_29)]
            ),
            i(59)
        );
    }
    #[test]
    fn unknown_unit() {
        assert!(eval_err("date_diff", &[s("year"), ts(0), ts(0)]).contains("unknown"));
    }
}

// ===========================================================================
// timestamp_add
// ===========================================================================
mod timestamp_add_tests {
    use super::*;

    #[test]
    fn add_days() {
        assert_eq!(
            eval("timestamp_add", &[s("day"), i(1), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + NANOS_PER_DAY)
        );
    }
    #[test]
    fn add_hours() {
        assert_eq!(
            eval("timestamp_add", &[s("hour"), i(5), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 5 * NANOS_PER_HOUR)
        );
    }
    #[test]
    fn add_minutes() {
        assert_eq!(
            eval("timestamp_add", &[s("minute"), i(30), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 30 * NANOS_PER_MIN)
        );
    }
    #[test]
    fn add_seconds() {
        assert_eq!(
            eval("timestamp_add", &[s("second"), i(60), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01 + 60 * NANOS_PER_SEC)
        );
    }
    #[test]
    fn add_months() {
        let r = eval("timestamp_add", &[s("month"), i(1), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(2));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn add_years() {
        let r = eval("timestamp_add", &[s("year"), i(1), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_year", &[r]), i(2025));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("timestamp_add", &[s("day"), i(1), null()]), null());
    }
    #[test]
    fn subtract_days() {
        let r = eval("timestamp_add", &[s("day"), i(-1), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_day", &[r.clone()]), i(31));
        assert_eq!(eval("extract_month", &[r]), i(12));
    }
    #[test]
    fn add_months_leap() {
        // Jan 31 + 1 month -> Feb 29 (2024 is leap year)
        let jan31 = TS_2024_01_01 + 30 * NANOS_PER_DAY;
        let r = eval("timestamp_add", &[s("month"), i(1), ts(jan31)]);
        assert_eq!(eval("extract_day", &[r.clone()]), i(29));
        assert_eq!(eval("extract_month", &[r]), i(2));
    }
    #[test]
    fn date_add_alias() {
        assert_eq!(
            eval("date_add", &[s("day"), i(1), ts(TS_2024_01_01)]),
            eval("timestamp_add", &[s("day"), i(1), ts(TS_2024_01_01)])
        );
    }
    #[test]
    fn dateadd_alias() {
        assert_eq!(
            eval("dateadd", &[s("day"), i(1), ts(TS_2024_01_01)]),
            eval("timestamp_add", &[s("day"), i(1), ts(TS_2024_01_01)])
        );
    }
    #[test]
    fn unknown_unit() {
        assert!(eval_err("timestamp_add", &[s("century"), i(1), ts(0)]).contains("unknown"));
    }
}

// ===========================================================================
// epoch_nanos / epoch_seconds / epoch_millis / epoch_micros
// ===========================================================================
mod epoch_tests {
    use super::*;

    #[test]
    fn nanos_basic() {
        assert_eq!(eval("epoch_nanos", &[ts(TS_2024_01_01)]), i(TS_2024_01_01));
    }
    #[test]
    fn nanos_null() {
        assert_eq!(eval("epoch_nanos", &[null()]), null());
    }
    #[test]
    fn nanos_epoch() {
        assert_eq!(eval("epoch_nanos", &[ts(0)]), i(0));
    }

    #[test]
    fn seconds_basic() {
        assert_eq!(
            eval("epoch_seconds", &[ts(TS_2024_01_01)]),
            i(TS_2024_01_01 / NANOS_PER_SEC)
        );
    }
    #[test]
    fn seconds_null() {
        assert_eq!(eval("epoch_seconds", &[null()]), null());
    }
    #[test]
    fn seconds_epoch() {
        assert_eq!(eval("epoch_seconds", &[ts(0)]), i(0));
    }
    #[test]
    fn unix_timestamp_alias() {
        assert_eq!(
            eval("unix_timestamp", &[ts(TS_2024_01_01)]),
            i(TS_2024_01_01 / NANOS_PER_SEC)
        );
    }
    #[test]
    fn nanos_to_secs_alias() {
        assert_eq!(
            eval("nanos_to_secs", &[ts(TS_2024_01_01)]),
            i(TS_2024_01_01 / NANOS_PER_SEC)
        );
    }

    #[test]
    fn millis_basic() {
        assert_eq!(
            eval("epoch_millis", &[ts(TS_2024_01_01)]),
            i(TS_2024_01_01 / 1_000_000)
        );
    }
    #[test]
    fn millis_null() {
        assert_eq!(eval("epoch_millis", &[null()]), null());
    }
    #[test]
    fn nanos_to_millis_alias() {
        assert_eq!(
            eval("nanos_to_millis", &[ts(TS_2024_01_01)]),
            i(TS_2024_01_01 / 1_000_000)
        );
    }

    #[test]
    fn micros_basic() {
        assert_eq!(
            eval("epoch_micros", &[ts(TS_2024_01_01)]),
            i(TS_2024_01_01 / 1_000)
        );
    }
    #[test]
    fn micros_null() {
        assert_eq!(eval("epoch_micros", &[null()]), null());
    }
    #[test]
    fn nanos_to_micros_alias() {
        assert_eq!(
            eval("nanos_to_micros", &[ts(TS_2024_01_01)]),
            i(TS_2024_01_01 / 1_000)
        );
    }
}

// ===========================================================================
// date_format
// ===========================================================================
mod date_format_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(
            eval("date_format", &[ts(TS_2024_03_15_123045), s("%Y-%m-%d")]),
            s("2024-03-15")
        );
    }
    #[test]
    fn with_time() {
        assert_eq!(
            eval(
                "date_format",
                &[ts(TS_2024_03_15_123045), s("%Y-%m-%d %H:%M:%S")]
            ),
            s("2024-03-15 12:30:45")
        );
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("date_format", &[null(), s("%Y")]), null());
    }
    #[test]
    fn year_only() {
        assert_eq!(
            eval("date_format", &[ts(TS_2024_01_01), s("%Y")]),
            s("2024")
        );
    }
    #[test]
    fn month_day() {
        assert_eq!(
            eval("date_format", &[ts(TS_2024_01_01), s("%m/%d")]),
            s("01/01")
        );
    }
    #[test]
    fn default_format() {
        let r = eval("date_format", &[ts(TS_2024_01_01)]);
        match r {
            Value::Str(s) => assert!(s.contains("2024")),
            _ => panic!(),
        }
    }
    #[test]
    fn strftime_alias() {
        assert_eq!(
            eval("strftime", &[ts(TS_2024_01_01), s("%Y-%m-%d")]),
            s("2024-01-01")
        );
    }
    #[test]
    fn timestamp_to_str_alias() {
        assert_eq!(
            eval("timestamp_to_str", &[ts(TS_2024_01_01), s("%Y-%m-%d")]),
            s("2024-01-01")
        );
    }
    #[test]
    fn format_timestamp_alias() {
        assert_eq!(
            eval("format_timestamp", &[ts(TS_2024_01_01), s("%Y-%m-%d")]),
            s("2024-01-01")
        );
    }
    #[test]
    fn epoch_format() {
        assert_eq!(
            eval("date_format", &[ts(EPOCH), s("%Y-%m-%d")]),
            s("1970-01-01")
        );
    }
}

// ===========================================================================
// make_timestamp
// ===========================================================================
mod make_timestamp_tests {
    use super::*;

    #[test]
    fn basic() {
        let r = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_year", &[r.clone()]), i(2024));
        assert_eq!(eval("extract_month", &[r.clone()]), i(1));
        assert_eq!(eval("extract_day", &[r.clone()]), i(1));
        assert_eq!(eval("extract_hour", &[r.clone()]), i(0));
    }
    #[test]
    fn with_time() {
        let r = eval(
            "make_timestamp",
            &[i(2024), i(3), i(15), i(12), i(30), i(45)],
        );
        assert_eq!(eval("extract_hour", &[r.clone()]), i(12));
        assert_eq!(eval("extract_minute", &[r.clone()]), i(30));
        assert_eq!(eval("extract_second", &[r]), i(45));
    }
    #[test]
    fn epoch() {
        let r = eval("make_timestamp", &[i(1970), i(1), i(1), i(0), i(0), i(0)]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, 0),
            _ => panic!(),
        }
    }
    #[test]
    fn leap_day() {
        let r = eval("make_timestamp", &[i(2024), i(2), i(29), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(2));
        assert_eq!(eval("extract_day", &[r]), i(29));
    }
    #[test]
    fn end_of_day() {
        let r = eval(
            "make_timestamp",
            &[i(2024), i(12), i(31), i(23), i(59), i(59)],
        );
        assert_eq!(eval("extract_hour", &[r.clone()]), i(23));
        assert_eq!(eval("extract_minute", &[r.clone()]), i(59));
        assert_eq!(eval("extract_second", &[r]), i(59));
    }
    #[test]
    fn y2000() {
        let r = eval(
            "make_timestamp",
            &[i(2000), i(6), i(15), i(10), i(20), i(30)],
        );
        assert_eq!(eval("extract_year", &[r.clone()]), i(2000));
        assert_eq!(eval("extract_month", &[r]), i(6));
    }
}

// ===========================================================================
// is_weekend / is_business_day
// ===========================================================================
mod weekend_tests {
    use super::*;

    #[test]
    fn monday_not_weekend() {
        assert_eq!(eval("is_weekend", &[ts(TS_2024_01_01)]), i(0));
    } // Monday
    #[test]
    fn saturday_is_weekend() {
        assert_eq!(eval("is_weekend", &[ts(TS_2024_06_15)]), i(1));
    } // Saturday
    #[test]
    fn null_input() {
        assert_eq!(eval("is_weekend", &[null()]), null());
    }
    #[test]
    fn thursday_not_weekend() {
        assert_eq!(eval("is_weekend", &[ts(TS_2024_02_29)]), i(0));
    } // Thursday
    #[test]
    fn sunday_is_weekend() {
        let sunday = TS_2024_06_15 + NANOS_PER_DAY; // June 16, 2024 = Sunday
        assert_eq!(eval("is_weekend", &[ts(sunday)]), i(1));
    }
    #[test]
    fn friday_not_weekend() {
        let friday = TS_2024_06_15 - NANOS_PER_DAY; // June 14, 2024 = Friday
        assert_eq!(eval("is_weekend", &[ts(friday)]), i(0));
    }

    #[test]
    fn monday_is_business() {
        assert_eq!(eval("is_business_day", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn saturday_not_business() {
        assert_eq!(eval("is_business_day", &[ts(TS_2024_06_15)]), i(0));
    }
    #[test]
    fn null_biz() {
        assert_eq!(eval("is_business_day", &[null()]), null());
    }
    #[test]
    fn thursday_is_business() {
        assert_eq!(eval("is_business_day", &[ts(TS_2024_02_29)]), i(1));
    }
}

// ===========================================================================
// first_of_month / last_of_month
// ===========================================================================
mod month_boundary_tests {
    use super::*;

    #[test]
    fn first_of_jan() {
        let r = eval("first_of_month", &[ts(TS_2024_01_01 + 15 * NANOS_PER_DAY)]);
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn first_of_feb() {
        let r = eval("first_of_month", &[ts(TS_2024_02_29)]);
        assert_eq!(eval("extract_day", &[r.clone()]), i(1));
        assert_eq!(eval("extract_month", &[r]), i(2));
    }
    #[test]
    fn first_null() {
        assert_eq!(eval("first_of_month", &[null()]), null());
    }
    #[test]
    fn first_day_of_month_alias() {
        let r = eval("first_day_of_month", &[ts(TS_2024_03_15_123045)]);
        assert_eq!(eval("extract_day", &[r]), i(1));
    }

    #[test]
    fn last_of_jan() {
        let r = eval("last_of_month", &[ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_day", &[r]), i(31));
    }
    #[test]
    fn last_of_feb_leap() {
        let r = eval("last_of_month", &[ts(TS_2024_02_29)]);
        assert_eq!(eval("extract_day", &[r]), i(29));
    }
    #[test]
    fn last_null() {
        assert_eq!(eval("last_of_month", &[null()]), null());
    }
    #[test]
    fn last_of_apr() {
        // April 2024 -> 30 days
        let apr = TS_2024_01_01 + 91 * NANOS_PER_DAY; // approx April
        let r = eval("last_of_month", &[ts(apr)]);
        assert_eq!(eval("extract_day", &[r]), i(30));
    }
    #[test]
    fn last_day_of_month_alias() {
        let r = eval("last_day_of_month", &[ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_day", &[r]), i(31));
    }
}

// ===========================================================================
// days_in_month_fn / is_leap_year_fn
// ===========================================================================
mod days_leap_tests {
    use super::*;

    #[test]
    fn days_jan() {
        assert_eq!(eval("days_in_month_fn", &[ts(TS_2024_01_01)]), i(31));
    }
    #[test]
    fn days_feb_leap() {
        assert_eq!(eval("days_in_month_fn", &[ts(TS_2024_02_29)]), i(29));
    }
    #[test]
    fn days_null() {
        assert_eq!(eval("days_in_month_fn", &[null()]), null());
    }
    #[test]
    fn days_apr() {
        let apr = TS_2024_01_01 + 91 * NANOS_PER_DAY;
        assert_eq!(eval("days_in_month_fn", &[ts(apr)]), i(30));
    }

    #[test]
    fn leap_2024() {
        assert_eq!(eval("is_leap_year_fn", &[ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn not_leap_2023() {
        assert_eq!(eval("is_leap_year_fn", &[ts(TS_2023_06_15)]), i(0));
    }
    #[test]
    fn leap_2000() {
        assert_eq!(eval("is_leap_year_fn", &[ts(TS_2000_01_01)]), i(1));
    }
    #[test]
    fn leap_null() {
        assert_eq!(eval("is_leap_year_fn", &[null()]), null());
    }
}

// ===========================================================================
// months_between / years_between
// ===========================================================================
mod between_date_tests {
    use super::*;

    #[test]
    fn months_same() {
        assert_eq!(
            eval("months_between", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]),
            i(0)
        );
    }
    #[test]
    fn months_one_year() {
        assert_eq!(
            eval("months_between", &[ts(TS_2024_01_01), ts(TS_2023_06_15)]),
            i(7)
        );
    }
    #[test]
    fn months_null() {
        assert_eq!(eval("months_between", &[null(), ts(TS_2024_01_01)]), null());
    }
    #[test]
    fn months_negative() {
        let r = eval("months_between", &[ts(TS_2024_01_01), ts(TS_2024_06_15)]);
        assert_eq!(r, i(-5));
    }
    #[test]
    fn months_two_years() {
        let r = eval("months_between", &[ts(TS_2024_01_01), ts(TS_2000_01_01)]);
        assert_eq!(r, i(288)); // 24 years * 12
    }

    #[test]
    fn years_same() {
        assert_eq!(
            eval("years_between", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]),
            i(0)
        );
    }
    #[test]
    fn years_24() {
        assert_eq!(
            eval("years_between", &[ts(TS_2024_01_01), ts(TS_2000_01_01)]),
            i(24)
        );
    }
    #[test]
    fn years_null() {
        assert_eq!(eval("years_between", &[null(), ts(TS_2024_01_01)]), null());
    }
    #[test]
    fn years_one() {
        assert_eq!(
            eval("years_between", &[ts(TS_2024_01_01), ts(TS_2023_06_15)]),
            i(1)
        );
    }
    #[test]
    fn years_negative() {
        assert_eq!(
            eval("years_between", &[ts(TS_2023_06_15), ts(TS_2024_01_01)]),
            i(-1)
        );
    }
}

// ===========================================================================
// timestamp_ceil
// ===========================================================================
mod timestamp_ceil_tests {
    use super::*;

    #[test]
    fn ceil_day_exact() {
        assert_eq!(
            eval("timestamp_ceil", &[s("day"), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01)
        );
    }
    #[test]
    fn ceil_day_rounds_up() {
        let mid = TS_2024_01_01 + NANOS_PER_HOUR;
        let r = eval("timestamp_ceil", &[s("day"), ts(mid)]);
        // Should be start of Jan 2
        assert_eq!(eval("extract_day", &[r]), i(2));
    }
    #[test]
    fn ceil_hour() {
        let t = TS_2024_01_01 + 30 * NANOS_PER_MIN;
        let r = eval("timestamp_ceil", &[s("hour"), ts(t)]);
        assert_eq!(eval("extract_minute", &[r]), i(0));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("timestamp_ceil", &[s("day"), null()]), null());
    }
    #[test]
    fn ceil_month() {
        let mid_jan = TS_2024_01_01 + 15 * NANOS_PER_DAY;
        let r = eval("timestamp_ceil", &[s("month"), ts(mid_jan)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(2));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn ceil_year() {
        let mid_year = TS_2024_06_15;
        let r = eval("timestamp_ceil", &[s("year"), ts(mid_year)]);
        assert_eq!(eval("extract_year", &[r.clone()]), i(2025));
        assert_eq!(eval("extract_month", &[r]), i(1));
    }
}

// ===========================================================================
// interval_to_nanos
// ===========================================================================
mod interval_to_nanos_tests {
    use super::*;

    #[test]
    fn one_second() {
        assert_eq!(eval("interval_to_nanos", &[s("1s")]), i(NANOS_PER_SEC));
    }
    #[test]
    fn one_minute() {
        assert_eq!(eval("interval_to_nanos", &[s("1m")]), i(NANOS_PER_MIN));
    }
    #[test]
    fn one_hour() {
        assert_eq!(eval("interval_to_nanos", &[s("1h")]), i(NANOS_PER_HOUR));
    }
    #[test]
    fn one_day() {
        assert_eq!(eval("interval_to_nanos", &[s("1d")]), i(NANOS_PER_DAY));
    }
    #[test]
    fn five_hundred_ms() {
        assert_eq!(eval("interval_to_nanos", &[s("500ms")]), i(500_000_000));
    }
    #[test]
    fn ten_seconds() {
        assert_eq!(
            eval("interval_to_nanos", &[s("10s")]),
            i(10 * NANOS_PER_SEC)
        );
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("interval_to_nanos", &[null()]), null());
    }
    #[test]
    fn nanoseconds() {
        assert_eq!(eval("interval_to_nanos", &[s("1000ns")]), i(1000));
    }
    #[test]
    fn microseconds() {
        assert_eq!(eval("interval_to_nanos", &[s("1000us")]), i(1_000_000));
    }
    #[test]
    fn two_days() {
        assert_eq!(eval("interval_to_nanos", &[s("2d")]), i(2 * NANOS_PER_DAY));
    }
}

// ===========================================================================
// age
// ===========================================================================
mod age_tests {
    use super::*;

    #[test]
    fn same_ts() {
        assert_eq!(eval("age", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]), i(0));
    }
    #[test]
    fn one_day() {
        // age(ts1, ts2) returns ts2 - ts1 in nanos
        assert_eq!(
            eval(
                "age",
                &[ts(TS_2024_01_01), ts(TS_2024_01_01 + NANOS_PER_DAY)]
            ),
            i(NANOS_PER_DAY)
        );
    }
    #[test]
    fn null_first() {
        assert_eq!(eval("age", &[null(), ts(0)]), null());
    }
    #[test]
    fn null_second() {
        assert_eq!(eval("age", &[ts(0), null()]), null());
    }
    #[test]
    fn one_hour() {
        assert_eq!(
            eval(
                "age",
                &[ts(TS_2024_01_01), ts(TS_2024_01_01 + NANOS_PER_HOUR)]
            ),
            i(NANOS_PER_HOUR)
        );
    }
    #[test]
    fn reverse_order() {
        // age(later, earlier) returns negative
        assert_eq!(
            eval(
                "age",
                &[ts(TS_2024_01_01 + NANOS_PER_DAY), ts(TS_2024_01_01)]
            ),
            i(-NANOS_PER_DAY)
        );
    }
}

// ===========================================================================
// to_timezone / from_utc / to_utc
// ===========================================================================
mod timezone_tests {
    use super::*;

    #[test]
    fn to_est() {
        let r = eval("to_timezone", &[ts(TS_2024_01_01), s("EST")]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, TS_2024_01_01 - 5 * NANOS_PER_HOUR),
            _ => panic!(),
        }
    }
    #[test]
    fn to_utc() {
        assert_eq!(
            eval("to_timezone", &[ts(TS_2024_01_01), s("UTC")]),
            ts(TS_2024_01_01)
        );
    }
    #[test]
    fn to_jst() {
        let r = eval("to_timezone", &[ts(TS_2024_01_01), s("JST")]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, TS_2024_01_01 + 9 * NANOS_PER_HOUR),
            _ => panic!(),
        }
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("to_timezone", &[null(), s("EST")]), null());
    }
    #[test]
    fn offset_plus_5() {
        let r = eval("to_timezone", &[ts(TS_2024_01_01), s("+05:00")]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, TS_2024_01_01 + 5 * NANOS_PER_HOUR),
            _ => panic!(),
        }
    }

    #[test]
    fn from_utc_est() {
        let r = eval("from_utc", &[ts(TS_2024_01_01), s("EST")]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, TS_2024_01_01 - 5 * NANOS_PER_HOUR),
            _ => panic!(),
        }
    }
    #[test]
    fn from_utc_null() {
        assert_eq!(eval("from_utc", &[null(), s("EST")]), null());
    }

    #[test]
    fn to_utc_fn() {
        let local_est = TS_2024_01_01 - 5 * NANOS_PER_HOUR;
        let r = eval("to_utc", &[ts(local_est), s("EST")]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, local_est + 5 * NANOS_PER_HOUR),
            _ => panic!(),
        }
    }
    #[test]
    fn to_utc_null() {
        assert_eq!(eval("to_utc", &[null(), s("EST")]), null());
    }
}

// ===========================================================================
// start_of_year / end_of_year / start_of_quarter / start_of_week
// ===========================================================================
mod boundary_tests {
    use super::*;

    #[test]
    fn start_of_year() {
        let r = eval("start_of_year", &[ts(TS_2024_06_15)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(1));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn start_of_year_null() {
        assert_eq!(eval("start_of_year", &[null()]), null());
    }
    #[test]
    fn first_day_of_year_alias() {
        let r = eval("first_day_of_year", &[ts(TS_2024_06_15)]);
        assert_eq!(eval("extract_month", &[r]), i(1));
    }

    #[test]
    fn end_of_year() {
        let r = eval("end_of_year", &[ts(TS_2024_06_15)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(12));
        assert_eq!(eval("extract_day", &[r]), i(31));
    }
    #[test]
    fn end_of_year_null() {
        assert_eq!(eval("end_of_year", &[null()]), null());
    }
    #[test]
    fn last_day_of_year_alias() {
        let r = eval("last_day_of_year", &[ts(TS_2024_06_15)]);
        assert_eq!(eval("extract_month", &[r]), i(12));
    }

    #[test]
    fn start_of_quarter_q1() {
        let r = eval("start_of_quarter", &[ts(TS_2024_02_29)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(1));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn start_of_quarter_q2() {
        let r = eval("start_of_quarter", &[ts(TS_2024_06_15)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(4));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn start_of_quarter_q4() {
        let r = eval("start_of_quarter", &[ts(TS_2024_12_31_235959)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(10));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn start_of_quarter_null() {
        assert_eq!(eval("start_of_quarter", &[null()]), null());
    }
    #[test]
    fn first_day_of_quarter_alias() {
        let r = eval("first_day_of_quarter", &[ts(TS_2024_06_15)]);
        assert_eq!(eval("extract_month", &[r]), i(4));
    }

    #[test]
    fn start_of_week() {
        // Jan 1 2024 is Monday, so start_of_week = Jan 1
        let r = eval("start_of_week", &[ts(TS_2024_01_01 + 3 * NANOS_PER_DAY)]); // Thursday Jan 4
        assert_eq!(eval("extract_day", &[r]), i(1)); // Monday Jan 1
    }
    #[test]
    fn start_of_week_null() {
        assert_eq!(eval("start_of_week", &[null()]), null());
    }
    #[test]
    fn first_day_of_week_alias() {
        let r = eval(
            "first_day_of_week",
            &[ts(TS_2024_01_01 + 3 * NANOS_PER_DAY)],
        );
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
}

// ===========================================================================
// to_date
// ===========================================================================
mod to_date_tests {
    use super::*;

    #[test]
    fn basic() {
        let r = eval("to_date", &[s("2024-01-01"), s("yyyy-mm-dd")]);
        assert_eq!(eval("extract_year", &[r.clone()]), i(2024));
        assert_eq!(eval("extract_month", &[r.clone()]), i(1));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn leap_day() {
        let r = eval("to_date", &[s("2024-02-29"), s("yyyy-mm-dd")]);
        assert_eq!(eval("extract_day", &[r]), i(29));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("to_date", &[null(), s("yyyy-mm-dd")]), null());
    }
    #[test]
    fn end_of_year() {
        let r = eval("to_date", &[s("2024-12-31"), s("yyyy-mm-dd")]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(12));
        assert_eq!(eval("extract_day", &[r]), i(31));
    }
    #[test]
    fn y2000() {
        let r = eval("to_date", &[s("2000-06-15"), s("yyyy-mm-dd")]);
        assert_eq!(eval("extract_year", &[r]), i(2000));
    }
}

// ===========================================================================
// next_day
// ===========================================================================
mod next_day_tests {
    use super::*;

    #[test]
    fn next_monday_from_monday() {
        // Jan 1 2024 is Monday, next Monday is Jan 8
        let r = eval("next_day", &[ts(TS_2024_01_01), s("Monday")]);
        assert_eq!(eval("extract_day", &[r]), i(8));
    }
    #[test]
    fn next_friday_from_monday() {
        let r = eval("next_day", &[ts(TS_2024_01_01), s("Friday")]);
        assert_eq!(eval("extract_day", &[r]), i(5));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("next_day", &[null(), s("Monday")]), null());
    }
    #[test]
    fn abbreviated_day() {
        let r = eval("next_day", &[ts(TS_2024_01_01), s("fri")]);
        assert_eq!(eval("extract_day", &[r]), i(5));
    }
    #[test]
    fn next_sunday() {
        let r = eval("next_day", &[ts(TS_2024_01_01), s("Sunday")]);
        assert_eq!(eval("extract_day", &[r]), i(7));
    }
    #[test]
    fn bad_day_name() {
        assert!(eval_err("next_day", &[ts(0), s("notaday")]).contains("unknown"));
    }
}

// ===========================================================================
// cast_to_timestamp / cast_timestamp
// ===========================================================================
mod cast_timestamp_tests {
    use super::*;

    #[test]
    fn from_int() {
        assert_eq!(eval("cast_to_timestamp", &[i(1000)]), ts(1000));
    }
    #[test]
    fn from_ts() {
        assert_eq!(eval("cast_to_timestamp", &[ts(1000)]), ts(1000));
    }
    #[test]
    fn from_null() {
        assert_eq!(eval("cast_to_timestamp", &[null()]), null());
    }
    #[test]
    fn cast_timestamp_alias() {
        assert_eq!(eval("cast_timestamp", &[i(1000)]), ts(1000));
    }
    #[test]
    fn from_float() {
        assert_eq!(eval("cast_to_timestamp", &[f(1000.0)]), ts(1000));
    }
    #[test]
    fn from_string() {
        assert_eq!(eval("cast_to_timestamp", &[s("1000")]), ts(1000));
    }
    #[test]
    fn from_iso() {
        let r = eval("cast_to_timestamp", &[s("2024-01-01")]);
        match r {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn from_iso_datetime() {
        let r = eval("cast_to_timestamp", &[s("2024-01-01T12:00:00")]);
        match r {
            Value::Timestamp(ns) => assert!(ns > 0),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// date_part (additional coverage)
// ===========================================================================
mod date_part_tests {
    use super::*;

    #[test]
    fn doy() {
        assert_eq!(eval("date_part", &[s("doy"), ts(TS_2024_01_01)]), i(1));
    }
    #[test]
    fn null_ts() {
        assert_eq!(eval("date_part", &[s("year"), null()]), null());
    }
    #[test]
    fn unknown_part() {
        assert!(eval_err("date_part", &[s("century"), ts(0)]).contains("unknown"));
    }
    #[test]
    fn quarter_q3() {
        // July 2024
        let july = TS_2024_06_15 + 16 * NANOS_PER_DAY;
        assert_eq!(eval("date_part", &[s("quarter"), ts(july)]), i(3));
    }
}

// ===========================================================================
// timestamp_sequence
// ===========================================================================
mod timestamp_sequence_tests {
    use super::*;

    #[test]
    fn returns_start() {
        let r = eval(
            "timestamp_sequence",
            &[ts(TS_2024_01_01), i(NANOS_PER_HOUR)],
        );
        assert_eq!(r, ts(TS_2024_01_01));
    }
    #[test]
    fn null_input() {
        assert_eq!(
            eval("timestamp_sequence", &[null(), i(NANOS_PER_HOUR)]),
            null()
        );
    }
}

// ===========================================================================
// rnd_timestamp (just returns a Timestamp)
// ===========================================================================
mod rnd_timestamp_tests {
    use super::*;

    #[test]
    fn returns_timestamp() {
        let r = eval(
            "rnd_timestamp",
            &[ts(TS_2024_01_01), ts(TS_2024_12_31_235959)],
        );
        match r {
            Value::Timestamp(ns) => assert!(ns >= TS_2024_01_01 && ns <= TS_2024_12_31_235959),
            _ => panic!(),
        }
    }
    #[test]
    fn same_bounds() {
        let r = eval("rnd_timestamp", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]);
        assert_eq!(r, ts(TS_2024_01_01));
    }
}

// ===========================================================================
// Additional date function coverage for higher test count
// ===========================================================================
mod additional_extract_tests {
    use super::*;

    // More extract_year
    #[test]
    fn year_1970() {
        assert_eq!(eval("extract_year", &[ts(0)]), i(1970));
    }
    #[test]
    fn year_from_make() {
        let t = eval(
            "make_timestamp",
            &[i(1999), i(12), i(31), i(23), i(59), i(59)],
        );
        assert_eq!(eval("extract_year", &[t]), i(1999));
    }
    #[test]
    fn year_2030() {
        let t = eval("make_timestamp", &[i(2030), i(6), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_year", &[t]), i(2030));
    }

    // More extract_month
    #[test]
    fn month_july() {
        let t = eval("make_timestamp", &[i(2024), i(7), i(4), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_month", &[t]), i(7));
    }
    #[test]
    fn month_november() {
        let t = eval("make_timestamp", &[i(2024), i(11), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_month", &[t]), i(11));
    }
    #[test]
    fn month_october() {
        let t = eval("make_timestamp", &[i(2024), i(10), i(15), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_month", &[t]), i(10));
    }

    // More extract_day
    #[test]
    fn day_28() {
        let t = eval("make_timestamp", &[i(2023), i(2), i(28), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_day", &[t]), i(28));
    }
    #[test]
    fn day_30() {
        let t = eval("make_timestamp", &[i(2024), i(4), i(30), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_day", &[t]), i(30));
    }

    // More extract_hour
    #[test]
    fn hour_6am() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(6), i(0), i(0)]);
        assert_eq!(eval("extract_hour", &[t]), i(6));
    }
    #[test]
    fn hour_18pm() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(18), i(0), i(0)]);
        assert_eq!(eval("extract_hour", &[t]), i(18));
    }

    // More extract_minute
    #[test]
    fn minute_45() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(45), i(0)]);
        assert_eq!(eval("extract_minute", &[t]), i(45));
    }
    #[test]
    fn minute_15() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(15), i(0)]);
        assert_eq!(eval("extract_minute", &[t]), i(15));
    }

    // More extract_second
    #[test]
    fn second_30() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(30)]);
        assert_eq!(eval("extract_second", &[t]), i(30));
    }
    #[test]
    fn second_1() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(1)]);
        assert_eq!(eval("extract_second", &[t]), i(1));
    }
}

mod additional_date_trunc_tests {
    use super::*;

    #[test]
    fn trunc_day_epoch() {
        let r = eval("date_trunc", &[s("day"), ts(NANOS_PER_HOUR * 5)]);
        assert_eq!(r, ts(0));
    }
    #[test]
    fn trunc_month_mid_month() {
        let t = eval(
            "make_timestamp",
            &[i(2024), i(6), i(15), i(12), i(30), i(45)],
        );
        let r = eval("date_trunc", &[s("month"), t]);
        assert_eq!(eval("extract_day", &[r.clone()]), i(1));
        assert_eq!(eval("extract_month", &[r]), i(6));
    }
    #[test]
    fn trunc_year_mid_year() {
        let t = eval("make_timestamp", &[i(2024), i(8), i(20), i(15), i(0), i(0)]);
        let r = eval("date_trunc", &[s("year"), t]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(1));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn trunc_hour_preserves_date() {
        let t = eval(
            "make_timestamp",
            &[i(2024), i(3), i(15), i(12), i(30), i(45)],
        );
        let r = eval("date_trunc", &[s("hour"), t]);
        assert_eq!(eval("extract_hour", &[r.clone()]), i(12));
        assert_eq!(eval("extract_minute", &[r.clone()]), i(0));
        assert_eq!(eval("extract_second", &[r]), i(0));
    }
    #[test]
    fn trunc_minute_preserves() {
        let t = eval(
            "make_timestamp",
            &[i(2024), i(3), i(15), i(12), i(30), i(45)],
        );
        let r = eval("date_trunc", &[s("minute"), t]);
        assert_eq!(eval("extract_minute", &[r.clone()]), i(30));
        assert_eq!(eval("extract_second", &[r]), i(0));
    }
}

mod additional_timestamp_add_tests {
    use super::*;

    #[test]
    fn add_zero_days() {
        assert_eq!(
            eval("timestamp_add", &[s("day"), i(0), ts(TS_2024_01_01)]),
            ts(TS_2024_01_01)
        );
    }
    #[test]
    fn add_365_days() {
        let r = eval("timestamp_add", &[s("day"), i(366), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_year", &[r]), i(2025)); // 2024 is leap year = 366 days
    }
    #[test]
    fn subtract_months() {
        let r = eval("timestamp_add", &[s("month"), i(-1), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(12));
        assert_eq!(eval("extract_year", &[r]), i(2023));
    }
    #[test]
    fn add_12_months() {
        let r = eval("timestamp_add", &[s("month"), i(12), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_year", &[r.clone()]), i(2025));
        assert_eq!(eval("extract_month", &[r]), i(1));
    }
    #[test]
    fn subtract_years() {
        let r = eval("timestamp_add", &[s("year"), i(-5), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_year", &[r]), i(2019));
    }
    #[test]
    fn add_24_hours() {
        let r = eval("timestamp_add", &[s("hour"), i(24), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_day", &[r]), i(2));
    }
    #[test]
    fn add_3600_seconds() {
        let r = eval("timestamp_add", &[s("second"), i(3600), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_hour", &[r]), i(1));
    }
    #[test]
    fn add_60_minutes() {
        let r = eval("timestamp_add", &[s("minute"), i(60), ts(TS_2024_01_01)]);
        assert_eq!(eval("extract_hour", &[r]), i(1));
    }
}

mod additional_date_diff_tests {
    use super::*;

    #[test]
    fn diff_one_day() {
        assert_eq!(
            eval(
                "date_diff",
                &[
                    s("day"),
                    ts(TS_2024_01_01),
                    ts(TS_2024_01_01 + NANOS_PER_DAY)
                ]
            ),
            i(1)
        );
    }
    #[test]
    fn diff_one_hour() {
        assert_eq!(
            eval(
                "date_diff",
                &[
                    s("hour"),
                    ts(TS_2024_01_01),
                    ts(TS_2024_01_01 + NANOS_PER_HOUR)
                ]
            ),
            i(1)
        );
    }
    #[test]
    fn diff_one_minute() {
        assert_eq!(
            eval(
                "date_diff",
                &[
                    s("minute"),
                    ts(TS_2024_01_01),
                    ts(TS_2024_01_01 + NANOS_PER_MIN)
                ]
            ),
            i(1)
        );
    }
    #[test]
    fn diff_one_second() {
        assert_eq!(
            eval(
                "date_diff",
                &[
                    s("second"),
                    ts(TS_2024_01_01),
                    ts(TS_2024_01_01 + NANOS_PER_SEC)
                ]
            ),
            i(1)
        );
    }
    #[test]
    fn diff_two_days() {
        assert_eq!(
            eval(
                "date_diff",
                &[
                    s("day"),
                    ts(TS_2024_01_01),
                    ts(TS_2024_01_01 + 2 * NANOS_PER_DAY)
                ]
            ),
            i(2)
        );
    }
    #[test]
    fn diff_half_day_in_hours() {
        assert_eq!(
            eval(
                "date_diff",
                &[
                    s("hour"),
                    ts(TS_2024_01_01),
                    ts(TS_2024_01_01 + 12 * NANOS_PER_HOUR)
                ]
            ),
            i(12)
        );
    }
    #[test]
    fn diff_zero() {
        assert_eq!(
            eval(
                "date_diff",
                &[s("second"), ts(TS_2024_01_01), ts(TS_2024_01_01)]
            ),
            i(0)
        );
    }
    #[test]
    fn diff_negative_one_day() {
        assert_eq!(
            eval(
                "date_diff",
                &[
                    s("day"),
                    ts(TS_2024_01_01 + NANOS_PER_DAY),
                    ts(TS_2024_01_01)
                ]
            ),
            i(-1)
        );
    }
}

mod additional_epoch_tests {
    use super::*;

    #[test]
    fn seconds_from_make() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        let secs = eval("epoch_seconds", &[t]);
        match secs {
            Value::I64(v) => assert!(v > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn millis_from_make() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        let ms = eval("epoch_millis", &[t]);
        match ms {
            Value::I64(v) => assert!(v > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn nanos_roundtrip() {
        let ns = eval("epoch_nanos", &[ts(TS_2024_01_01)]);
        assert_eq!(ns, i(TS_2024_01_01));
    }
    #[test]
    fn seconds_times_billion_equals_nanos() {
        let secs = match eval("epoch_seconds", &[ts(TS_2024_01_01)]) {
            Value::I64(v) => v,
            _ => panic!(),
        };
        assert_eq!(secs * NANOS_PER_SEC, TS_2024_01_01);
    }
}

mod additional_make_timestamp_tests {
    use super::*;

    #[test]
    fn midnight() {
        let t = eval("make_timestamp", &[i(2024), i(6), i(15), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_hour", &[t.clone()]), i(0));
        assert_eq!(eval("extract_minute", &[t.clone()]), i(0));
        assert_eq!(eval("extract_second", &[t]), i(0));
    }
    #[test]
    fn just_before_midnight() {
        let t = eval(
            "make_timestamp",
            &[i(2024), i(6), i(15), i(23), i(59), i(59)],
        );
        assert_eq!(eval("extract_hour", &[t.clone()]), i(23));
        assert_eq!(eval("extract_minute", &[t.clone()]), i(59));
        assert_eq!(eval("extract_second", &[t]), i(59));
    }
    #[test]
    fn february_non_leap() {
        let t = eval("make_timestamp", &[i(2023), i(2), i(28), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_day", &[t]), i(28));
    }
    #[test]
    fn december_end() {
        let t = eval("make_timestamp", &[i(2024), i(12), i(31), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_month", &[t.clone()]), i(12));
        assert_eq!(eval("extract_day", &[t]), i(31));
    }
    #[test]
    fn various_hours() {
        for h in [1, 5, 10, 15, 20, 23] {
            let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(h), i(0), i(0)]);
            assert_eq!(eval("extract_hour", &[t]), i(h));
        }
    }
    #[test]
    fn various_minutes() {
        for m in [0, 1, 15, 30, 45, 59] {
            let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(m), i(0)]);
            assert_eq!(eval("extract_minute", &[t]), i(m));
        }
    }
    #[test]
    fn various_months() {
        for m in 1..=12 {
            let t = eval("make_timestamp", &[i(2024), i(m), i(1), i(0), i(0), i(0)]);
            assert_eq!(eval("extract_month", &[t]), i(m));
        }
    }
}

mod additional_weekend_tests {
    use super::*;

    #[test]
    fn week_loop() {
        // Starting from Jan 1 2024 (Monday), check 7 days
        let mut weekdays = 0i64;
        let mut weekend = 0i64;
        for d in 0..7 {
            let t = ts(TS_2024_01_01 + d * NANOS_PER_DAY);
            match eval("is_weekend", &[t]) {
                Value::I64(0) => weekdays += 1,
                Value::I64(1) => weekend += 1,
                _ => panic!(),
            }
        }
        assert_eq!(weekdays, 5);
        assert_eq!(weekend, 2);
    }
    #[test]
    fn business_day_week() {
        let mut biz = 0i64;
        for d in 0..7 {
            let t = ts(TS_2024_01_01 + d * NANOS_PER_DAY);
            if let Value::I64(1) = eval("is_business_day", &[t]) {
                biz += 1;
            }
        }
        assert_eq!(biz, 5);
    }
}

mod additional_boundary_tests {
    use super::*;

    #[test]
    fn first_of_month_each_month() {
        for m in 1..=12 {
            let t = eval("make_timestamp", &[i(2024), i(m), i(15), i(0), i(0), i(0)]);
            let r = eval("first_of_month", &[t]);
            assert_eq!(eval("extract_day", &[r.clone()]), i(1));
            assert_eq!(eval("extract_month", &[r]), i(m));
        }
    }
    #[test]
    fn last_of_month_jan() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(15), i(0), i(0), i(0)]);
        let r = eval("last_of_month", &[t]);
        assert_eq!(eval("extract_day", &[r]), i(31));
    }
    #[test]
    fn last_of_month_feb_leap() {
        let t = eval("make_timestamp", &[i(2024), i(2), i(15), i(0), i(0), i(0)]);
        let r = eval("last_of_month", &[t]);
        assert_eq!(eval("extract_day", &[r]), i(29));
    }
    #[test]
    fn last_of_month_feb_non_leap() {
        let t = eval("make_timestamp", &[i(2023), i(2), i(15), i(0), i(0), i(0)]);
        let r = eval("last_of_month", &[t]);
        assert_eq!(eval("extract_day", &[r]), i(28));
    }
    #[test]
    fn last_of_month_apr() {
        let t = eval("make_timestamp", &[i(2024), i(4), i(10), i(0), i(0), i(0)]);
        let r = eval("last_of_month", &[t]);
        assert_eq!(eval("extract_day", &[r]), i(30));
    }
    #[test]
    fn days_in_month_all_months() {
        let expected = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]; // 2024 leap
        for (idx, &exp) in expected.iter().enumerate() {
            let m = (idx + 1) as i64;
            let t = eval("make_timestamp", &[i(2024), i(m), i(1), i(0), i(0), i(0)]);
            assert_eq!(eval("days_in_month_fn", &[t]), i(exp as i64));
        }
    }
    #[test]
    fn leap_year_2000() {
        let t = eval("make_timestamp", &[i(2000), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("is_leap_year_fn", &[t]), i(1));
    }
    #[test]
    fn not_leap_1900() {
        let t = eval("make_timestamp", &[i(1900), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("is_leap_year_fn", &[t]), i(0));
    }
    #[test]
    fn leap_2020() {
        let t = eval("make_timestamp", &[i(2020), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("is_leap_year_fn", &[t]), i(1));
    }
    #[test]
    fn not_leap_2023() {
        let t = eval("make_timestamp", &[i(2023), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("is_leap_year_fn", &[t]), i(0));
    }
}

mod additional_interval_tests {
    use super::*;

    #[test]
    fn thirty_min() {
        assert_eq!(
            eval("interval_to_nanos", &[s("30m")]),
            i(30 * NANOS_PER_MIN)
        );
    }
    #[test]
    fn two_hours() {
        assert_eq!(eval("interval_to_nanos", &[s("2h")]), i(2 * NANOS_PER_HOUR));
    }
    #[test]
    fn seven_days() {
        assert_eq!(eval("interval_to_nanos", &[s("7d")]), i(7 * NANOS_PER_DAY));
    }
    #[test]
    fn one_ms() {
        assert_eq!(eval("interval_to_nanos", &[s("1ms")]), i(1_000_000));
    }
    #[test]
    fn one_us() {
        assert_eq!(eval("interval_to_nanos", &[s("1us")]), i(1_000));
    }
    #[test]
    fn hundred_ns() {
        assert_eq!(eval("interval_to_nanos", &[s("100ns")]), i(100));
    }
    #[test]
    fn zero_seconds() {
        assert_eq!(eval("interval_to_nanos", &[s("0s")]), i(0));
    }
    #[test]
    fn sixty_seconds() {
        assert_eq!(
            eval("interval_to_nanos", &[s("60s")]),
            i(60 * NANOS_PER_SEC)
        );
    }
}

mod additional_timezone_tests {
    use super::*;

    #[test]
    fn pst() {
        let r = eval("to_timezone", &[ts(TS_2024_01_01), s("PST")]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, TS_2024_01_01 - 8 * NANOS_PER_HOUR),
            _ => panic!(),
        }
    }
    #[test]
    fn cet() {
        let r = eval("to_timezone", &[ts(TS_2024_01_01), s("CET")]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, TS_2024_01_01 + 1 * NANOS_PER_HOUR),
            _ => panic!(),
        }
    }
    #[test]
    fn gmt() {
        assert_eq!(
            eval("to_timezone", &[ts(TS_2024_01_01), s("GMT")]),
            ts(TS_2024_01_01)
        );
    }
    #[test]
    fn negative_offset() {
        let r = eval("to_timezone", &[ts(TS_2024_01_01), s("-03:00")]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, TS_2024_01_01 - 3 * NANOS_PER_HOUR),
            _ => panic!(),
        }
    }
    #[test]
    fn positive_offset() {
        let r = eval("to_timezone", &[ts(TS_2024_01_01), s("+09:00")]);
        match r {
            Value::Timestamp(ns) => assert_eq!(ns, TS_2024_01_01 + 9 * NANOS_PER_HOUR),
            _ => panic!(),
        }
    }
    #[test]
    fn utc_roundtrip() {
        let local = eval("from_utc", &[ts(TS_2024_01_01), s("EST")]);
        let utc = eval("to_utc", &[local, s("EST")]);
        assert_eq!(utc, ts(TS_2024_01_01));
    }
}

mod additional_format_tests {
    use super::*;

    #[test]
    fn format_epoch() {
        assert_eq!(
            eval("date_format", &[ts(0), s("%Y-%m-%d")]),
            s("1970-01-01")
        );
    }
    #[test]
    fn format_year_only() {
        let t = eval("make_timestamp", &[i(2024), i(6), i(15), i(0), i(0), i(0)]);
        assert_eq!(eval("date_format", &[t, s("%Y")]), s("2024"));
    }
    #[test]
    fn format_month_day() {
        let t = eval("make_timestamp", &[i(2024), i(3), i(5), i(0), i(0), i(0)]);
        assert_eq!(eval("date_format", &[t, s("%m-%d")]), s("03-05"));
    }
    #[test]
    fn format_time_only() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(14), i(30), i(0)]);
        assert_eq!(eval("date_format", &[t, s("%H:%M:%S")]), s("14:30:00"));
    }
    #[test]
    fn to_str_timestamp_format() {
        let t = eval(
            "make_timestamp",
            &[i(2024), i(12), i(25), i(10), i(0), i(0)],
        );
        assert_eq!(
            eval("to_str_timestamp", &[t, s("YYYY-MM-DD")]),
            s("2024-12-25")
        );
    }
}

mod additional_age_tests {
    use super::*;

    #[test]
    fn age_one_hour() {
        assert_eq!(
            eval(
                "age",
                &[ts(TS_2024_01_01), ts(TS_2024_01_01 + NANOS_PER_HOUR)]
            ),
            i(NANOS_PER_HOUR)
        );
    }
    #[test]
    fn age_30_days() {
        assert_eq!(
            eval(
                "age",
                &[ts(TS_2024_01_01), ts(TS_2024_01_01 + 30 * NANOS_PER_DAY)]
            ),
            i(30 * NANOS_PER_DAY)
        );
    }
    #[test]
    fn age_same_timestamp() {
        assert_eq!(eval("age", &[ts(TS_2024_01_01), ts(TS_2024_01_01)]), i(0));
    }
    #[test]
    fn age_one_minute() {
        assert_eq!(
            eval(
                "age",
                &[ts(TS_2024_01_01), ts(TS_2024_01_01 + NANOS_PER_MIN)]
            ),
            i(NANOS_PER_MIN)
        );
    }
}

mod additional_next_day_tests {
    use super::*;

    #[test]
    fn next_tuesday() {
        let r = eval("next_day", &[ts(TS_2024_01_01), s("Tuesday")]);
        assert_eq!(eval("extract_day", &[r]), i(2));
    }
    #[test]
    fn next_wednesday() {
        let r = eval("next_day", &[ts(TS_2024_01_01), s("Wednesday")]);
        assert_eq!(eval("extract_day", &[r]), i(3));
    }
    #[test]
    fn next_thursday() {
        let r = eval("next_day", &[ts(TS_2024_01_01), s("Thursday")]);
        assert_eq!(eval("extract_day", &[r]), i(4));
    }
    #[test]
    fn next_saturday() {
        let r = eval("next_day", &[ts(TS_2024_01_01), s("Saturday")]);
        assert_eq!(eval("extract_day", &[r]), i(6));
    }
    #[test]
    fn abbreviated_mon() {
        let r = eval("next_day", &[ts(TS_2024_01_01), s("mon")]);
        assert_eq!(eval("extract_day", &[r]), i(8));
    }
    #[test]
    fn abbreviated_sat() {
        let r = eval("next_day", &[ts(TS_2024_01_01), s("sat")]);
        assert_eq!(eval("extract_day", &[r]), i(6));
    }
}

mod additional_quarter_tests {
    use super::*;

    #[test]
    fn q1_jan() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(15), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_quarter", &[t]), i(1));
    }
    #[test]
    fn q1_mar() {
        let t = eval("make_timestamp", &[i(2024), i(3), i(31), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_quarter", &[t]), i(1));
    }
    #[test]
    fn q2_apr() {
        let t = eval("make_timestamp", &[i(2024), i(4), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_quarter", &[t]), i(2));
    }
    #[test]
    fn q2_jun() {
        let t = eval("make_timestamp", &[i(2024), i(6), i(30), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_quarter", &[t]), i(2));
    }
    #[test]
    fn q3_jul() {
        let t = eval("make_timestamp", &[i(2024), i(7), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_quarter", &[t]), i(3));
    }
    #[test]
    fn q3_sep() {
        let t = eval("make_timestamp", &[i(2024), i(9), i(30), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_quarter", &[t]), i(3));
    }
    #[test]
    fn q4_oct() {
        let t = eval("make_timestamp", &[i(2024), i(10), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_quarter", &[t]), i(4));
    }
    #[test]
    fn q4_dec() {
        let t = eval("make_timestamp", &[i(2024), i(12), i(31), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_quarter", &[t]), i(4));
    }
}

mod additional_start_of_quarter_tests {
    use super::*;

    #[test]
    fn q1_start() {
        let t = eval("make_timestamp", &[i(2024), i(2), i(15), i(0), i(0), i(0)]);
        let r = eval("start_of_quarter", &[t]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(1));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn q3_start() {
        let t = eval("make_timestamp", &[i(2024), i(8), i(15), i(0), i(0), i(0)]);
        let r = eval("start_of_quarter", &[t]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(7));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
    #[test]
    fn q4_start() {
        let t = eval("make_timestamp", &[i(2024), i(11), i(15), i(0), i(0), i(0)]);
        let r = eval("start_of_quarter", &[t]);
        assert_eq!(eval("extract_month", &[r.clone()]), i(10));
        assert_eq!(eval("extract_day", &[r]), i(1));
    }
}

mod additional_extract_deep_tests {
    use super::*;

    #[test]
    fn year_1980() {
        let t = eval("make_timestamp", &[i(1980), i(6), i(15), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_year", &[t]), i(1980));
    }
    #[test]
    fn year_2050() {
        let t = eval("make_timestamp", &[i(2050), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_year", &[t]), i(2050));
    }
    #[test]
    fn month_aug() {
        let t = eval("make_timestamp", &[i(2024), i(8), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_month", &[t]), i(8));
    }
    #[test]
    fn month_sep() {
        let t = eval("make_timestamp", &[i(2024), i(9), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("extract_month", &[t]), i(9));
    }
    #[test]
    fn day_15_each_month() {
        for m in 1..=12 {
            let t = eval("make_timestamp", &[i(2024), i(m), i(15), i(0), i(0), i(0)]);
            assert_eq!(eval("extract_day", &[t]), i(15));
        }
    }
    #[test]
    fn hour_each_3h() {
        for h in [0, 3, 6, 9, 12, 15, 18, 21] {
            let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(h), i(0), i(0)]);
            assert_eq!(eval("extract_hour", &[t]), i(h));
        }
    }
    #[test]
    fn minute_various() {
        for m in [0, 10, 20, 30, 40, 50, 59] {
            let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(m), i(0)]);
            assert_eq!(eval("extract_minute", &[t]), i(m));
        }
    }
    #[test]
    fn second_various() {
        for s in [0, 10, 20, 30, 40, 50, 59] {
            let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(s)]);
            assert_eq!(eval("extract_second", &[t]), i(s));
        }
    }
    #[test]
    fn extract_day_of_week_full_week() {
        // Jan 1 2024 = Monday (dow=1)
        for d in 0..7_i64 {
            let t = ts(TS_2024_01_01 + d * NANOS_PER_DAY);
            let dow = eval("extract_day_of_week", &[t]);
            match dow {
                Value::I64(v) => assert!(v >= 0 && v <= 6),
                _ => panic!(),
            }
        }
    }
    #[test]
    fn extract_quarter_all_12_months() {
        let expected = [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4];
        for m in 1..=12 {
            let t = eval("make_timestamp", &[i(2024), i(m), i(1), i(0), i(0), i(0)]);
            assert_eq!(eval("extract_quarter", &[t]), i(expected[(m - 1) as usize]));
        }
    }
}

mod additional_date_diff_deep_tests {
    use super::*;

    #[test]
    fn diff_365_days() {
        let a = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        let b = eval("make_timestamp", &[i(2025), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("date_diff", &[s("day"), a, b]), i(366)); // 2024 is leap
    }
    #[test]
    fn diff_hours_across_day() {
        let a = eval("make_timestamp", &[i(2024), i(1), i(1), i(22), i(0), i(0)]);
        let b = eval("make_timestamp", &[i(2024), i(1), i(2), i(2), i(0), i(0)]);
        assert_eq!(eval("date_diff", &[s("hour"), a, b]), i(4));
    }
    #[test]
    fn diff_minutes_90() {
        let a = ts(TS_2024_01_01);
        let b = ts(TS_2024_01_01 + 90 * NANOS_PER_MIN);
        assert_eq!(eval("date_diff", &[s("minute"), a, b]), i(90));
    }
    #[test]
    fn diff_seconds_3600() {
        let a = ts(TS_2024_01_01);
        let b = ts(TS_2024_01_01 + 3600 * NANOS_PER_SEC);
        assert_eq!(eval("date_diff", &[s("second"), a, b]), i(3600));
    }
}

mod additional_date_format_deep_tests {
    use super::*;

    #[test]
    fn format_all_zeros() {
        let t = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("date_format", &[t, s("%H:%M:%S")]), s("00:00:00"));
    }
    #[test]
    fn format_max_time() {
        let t = eval(
            "make_timestamp",
            &[i(2024), i(1), i(1), i(23), i(59), i(59)],
        );
        assert_eq!(eval("date_format", &[t, s("%H:%M:%S")]), s("23:59:59"));
    }
    #[test]
    fn format_date_slash() {
        let t = eval("make_timestamp", &[i(2024), i(6), i(15), i(0), i(0), i(0)]);
        assert_eq!(eval("date_format", &[t, s("%m/%d/%Y")]), s("06/15/2024"));
    }
    #[test]
    fn format_iso() {
        let t = eval(
            "make_timestamp",
            &[i(2024), i(3), i(20), i(14), i(30), i(0)],
        );
        assert_eq!(
            eval("date_format", &[t, s("%Y-%m-%d %H:%M:%S")]),
            s("2024-03-20 14:30:00")
        );
    }
}

mod additional_months_years_deep_tests {
    use super::*;

    #[test]
    fn months_between_same_month() {
        let a = eval("make_timestamp", &[i(2024), i(3), i(1), i(0), i(0), i(0)]);
        let b = eval("make_timestamp", &[i(2024), i(3), i(31), i(0), i(0), i(0)]);
        assert_eq!(eval("months_between", &[a, b]), i(0));
    }
    #[test]
    fn months_between_12() {
        let a = eval("make_timestamp", &[i(2025), i(1), i(1), i(0), i(0), i(0)]);
        let b = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("months_between", &[a, b]), i(12));
    }
    #[test]
    fn years_between_10() {
        let a = eval("make_timestamp", &[i(2034), i(1), i(1), i(0), i(0), i(0)]);
        let b = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        assert_eq!(eval("years_between", &[a, b]), i(10));
    }
    #[test]
    fn years_between_zero() {
        let a = eval("make_timestamp", &[i(2024), i(1), i(1), i(0), i(0), i(0)]);
        let b = eval("make_timestamp", &[i(2024), i(12), i(31), i(0), i(0), i(0)]);
        assert_eq!(eval("years_between", &[a, b]), i(0));
    }
}
