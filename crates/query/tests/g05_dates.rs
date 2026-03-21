//! 500 date extraction tests.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn i(v: i64) -> Value { Value::I64(v) }
fn ts(ns: i64) -> Value { Value::Timestamp(ns) }
fn null() -> Value { Value::Null }
fn ev(name: &str, args: &[Value]) -> Value { evaluate_scalar(name, args).unwrap() }

const NPS: i64 = 1_000_000_000;
const NPH: i64 = 3600 * NPS;
const NPD: i64 = 86400 * NPS;

// Unix timestamps for Jan 1 of each year (UTC midnight)
const TS_2020: i64 = 1577836800 * NPS;
const TS_2021: i64 = 1609459200 * NPS;
const TS_2022: i64 = 1640995200 * NPS;
const TS_2023: i64 = 1672531200 * NPS;
const TS_2024: i64 = 1704067200 * NPS;
const TS_2025: i64 = 1735689600 * NPS;

// extract_year for each year's Jan 1
mod year_basic { use super::*;
    #[test] fn y2020() { assert_eq!(ev("extract_year", &[ts(TS_2020)]), i(2020)); }
    #[test] fn y2021() { assert_eq!(ev("extract_year", &[ts(TS_2021)]), i(2021)); }
    #[test] fn y2022() { assert_eq!(ev("extract_year", &[ts(TS_2022)]), i(2022)); }
    #[test] fn y2023() { assert_eq!(ev("extract_year", &[ts(TS_2023)]), i(2023)); }
    #[test] fn y2024() { assert_eq!(ev("extract_year", &[ts(TS_2024)]), i(2024)); }
    #[test] fn y2025() { assert_eq!(ev("extract_year", &[ts(TS_2025)]), i(2025)); }
    #[test] fn null_in() { assert_eq!(ev("extract_year", &[null()]), null()); }
}

// extract_year at various day offsets in 2024 (leap year: 366 days)
mod year_2024 { use super::*;
    macro_rules! yr { ($n:ident, $off:expr, $y:expr) => { #[test] fn $n() { assert_eq!(ev("extract_year", &[ts(TS_2024 + $off * NPD)]), i($y)); } }; }
    yr!(d000, 0, 2024); yr!(d030, 30, 2024); yr!(d059, 59, 2024); yr!(d060, 60, 2024);
    yr!(d090, 90, 2024); yr!(d120, 120, 2024); yr!(d150, 150, 2024); yr!(d180, 180, 2024);
    yr!(d210, 210, 2024); yr!(d240, 240, 2024); yr!(d270, 270, 2024); yr!(d300, 300, 2024);
    yr!(d330, 330, 2024); yr!(d364, 364, 2024); yr!(d365, 365, 2024); yr!(d366, 366, 2025);
    yr!(d001, 1, 2024); yr!(d010, 10, 2024); yr!(d050, 50, 2024); yr!(d100, 100, 2024);
    yr!(d200, 200, 2024); yr!(d250, 250, 2024); yr!(d350, 350, 2024);
}

// extract_month for each month in 2024
mod month_2024 { use super::*;
    // 2024 is leap: Jan=31, Feb=29, Mar=31, Apr=30, May=31, Jun=30, Jul=31, Aug=31, Sep=30, Oct=31, Nov=30, Dec=31
    // Cumulative days: Jan=0, Feb=31, Mar=60, Apr=91, May=121, Jun=152, Jul=182, Aug=213, Sep=244, Oct=274, Nov=305, Dec=335
    macro_rules! mo { ($n:ident, $off:expr, $m:expr) => { #[test] fn $n() { assert_eq!(ev("extract_month", &[ts(TS_2024 + $off * NPD)]), i($m)); } }; }
    mo!(jan_1, 0, 1); mo!(jan_15, 14, 1); mo!(jan_31, 30, 1);
    mo!(feb_1, 31, 2); mo!(feb_15, 45, 2); mo!(feb_29, 59, 2);
    mo!(mar_1, 60, 3); mo!(mar_15, 74, 3); mo!(mar_31, 90, 3);
    mo!(apr_1, 91, 4); mo!(apr_15, 105, 4); mo!(apr_30, 120, 4);
    mo!(may_1, 121, 5); mo!(may_15, 135, 5); mo!(may_31, 151, 5);
    mo!(jun_1, 152, 6); mo!(jun_15, 166, 6); mo!(jun_30, 181, 6);
    mo!(jul_1, 182, 7); mo!(jul_15, 196, 7); mo!(jul_31, 212, 7);
    mo!(aug_1, 213, 8); mo!(aug_15, 227, 8); mo!(aug_31, 243, 8);
    mo!(sep_1, 244, 9); mo!(sep_15, 258, 9); mo!(sep_30, 273, 9);
    mo!(oct_1, 274, 10); mo!(oct_15, 288, 10); mo!(oct_31, 304, 10);
    mo!(nov_1, 305, 11); mo!(nov_15, 319, 11); mo!(nov_30, 334, 11);
    mo!(dec_1, 335, 12); mo!(dec_15, 349, 12); mo!(dec_31, 365, 12);
    #[test] fn null_in() { assert_eq!(ev("extract_month", &[null()]), null()); }
}

// extract_day for Jan 1-31 of 2024
mod day_jan { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 0, 1); dy!(d02, 1, 2); dy!(d03, 2, 3); dy!(d04, 3, 4); dy!(d05, 4, 5);
    dy!(d06, 5, 6); dy!(d07, 6, 7); dy!(d08, 7, 8); dy!(d09, 8, 9); dy!(d10, 9, 10);
    dy!(d11, 10, 11); dy!(d12, 11, 12); dy!(d13, 12, 13); dy!(d14, 13, 14); dy!(d15, 14, 15);
    dy!(d16, 15, 16); dy!(d17, 16, 17); dy!(d18, 17, 18); dy!(d19, 18, 19); dy!(d20, 19, 20);
    dy!(d21, 20, 21); dy!(d22, 21, 22); dy!(d23, 22, 23); dy!(d24, 23, 24); dy!(d25, 24, 25);
    dy!(d26, 25, 26); dy!(d27, 26, 27); dy!(d28, 27, 28); dy!(d29, 28, 29); dy!(d30, 29, 30);
    dy!(d31, 30, 31);
}

// extract_day for Feb 1-29 of 2024 (leap year)
mod day_feb { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 31, 1); dy!(d02, 32, 2); dy!(d03, 33, 3); dy!(d04, 34, 4); dy!(d05, 35, 5);
    dy!(d06, 36, 6); dy!(d07, 37, 7); dy!(d08, 38, 8); dy!(d09, 39, 9); dy!(d10, 40, 10);
    dy!(d11, 41, 11); dy!(d12, 42, 12); dy!(d13, 43, 13); dy!(d14, 44, 14); dy!(d15, 45, 15);
    dy!(d16, 46, 16); dy!(d17, 47, 17); dy!(d18, 48, 18); dy!(d19, 49, 19); dy!(d20, 50, 20);
    dy!(d21, 51, 21); dy!(d22, 52, 22); dy!(d23, 53, 23); dy!(d24, 54, 24); dy!(d25, 55, 25);
    dy!(d26, 56, 26); dy!(d27, 57, 27); dy!(d28, 58, 28); dy!(d29, 59, 29);
}

// extract_day for Mar 1-31
mod day_mar { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 60, 1); dy!(d02, 61, 2); dy!(d03, 62, 3); dy!(d04, 63, 4); dy!(d05, 64, 5);
    dy!(d06, 65, 6); dy!(d07, 66, 7); dy!(d08, 67, 8); dy!(d09, 68, 9); dy!(d10, 69, 10);
    dy!(d11, 70, 11); dy!(d12, 71, 12); dy!(d13, 72, 13); dy!(d14, 73, 14); dy!(d15, 74, 15);
    dy!(d16, 75, 16); dy!(d17, 76, 17); dy!(d18, 77, 18); dy!(d19, 78, 19); dy!(d20, 79, 20);
    dy!(d21, 80, 21); dy!(d22, 81, 22); dy!(d23, 82, 23); dy!(d24, 83, 24); dy!(d25, 84, 25);
    dy!(d26, 85, 26); dy!(d27, 86, 27); dy!(d28, 87, 28); dy!(d29, 88, 29); dy!(d30, 89, 30);
    dy!(d31, 90, 31);
}

// extract_day for April
mod day_apr { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 91, 1); dy!(d10, 100, 10); dy!(d15, 105, 15); dy!(d20, 110, 20); dy!(d30, 120, 30);
}

// extract_day for May
mod day_may { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 121, 1); dy!(d10, 130, 10); dy!(d15, 135, 15); dy!(d20, 140, 20); dy!(d31, 151, 31);
}

// extract_day for Jun
mod day_jun { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 152, 1); dy!(d15, 166, 15); dy!(d30, 181, 30);
}

// extract_day for Jul
mod day_jul { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 182, 1); dy!(d15, 196, 15); dy!(d31, 212, 31);
}

// extract_day for Aug
mod day_aug { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 213, 1); dy!(d15, 227, 15); dy!(d31, 243, 31);
}

// extract_day for Sep
mod day_sep { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 244, 1); dy!(d15, 258, 15); dy!(d30, 273, 30);
}

// extract_day for Oct
mod day_oct { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 274, 1); dy!(d15, 288, 15); dy!(d31, 304, 31);
}

// extract_day for Nov
mod day_nov { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 305, 1); dy!(d15, 319, 15); dy!(d30, 334, 30);
}

// extract_day for Dec
mod day_dec { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2024 + $off * NPD)]), i($d)); } }; }
    dy!(d01, 335, 1); dy!(d15, 349, 15); dy!(d31, 365, 31);
}

#[test] fn day_null() { assert_eq!(ev("extract_day", &[null()]), null()); }

// extract_hour 0-23
mod hour_tests { use super::*;
    macro_rules! hr { ($n:ident, $h:expr) => { #[test] fn $n() { assert_eq!(ev("extract_hour", &[ts(TS_2024 + $h * NPH)]), i($h)); } }; }
    hr!(h00, 0); hr!(h01, 1); hr!(h02, 2); hr!(h03, 3); hr!(h04, 4); hr!(h05, 5);
    hr!(h06, 6); hr!(h07, 7); hr!(h08, 8); hr!(h09, 9); hr!(h10, 10); hr!(h11, 11);
    hr!(h12, 12); hr!(h13, 13); hr!(h14, 14); hr!(h15, 15); hr!(h16, 16); hr!(h17, 17);
    hr!(h18, 18); hr!(h19, 19); hr!(h20, 20); hr!(h21, 21); hr!(h22, 22); hr!(h23, 23);
    #[test] fn null_in() { assert_eq!(ev("extract_hour", &[null()]), null()); }
}

// extract_month for 2020-2023
mod month_2020 { use super::*;
    macro_rules! mo { ($n:ident, $off:expr, $m:expr) => { #[test] fn $n() { assert_eq!(ev("extract_month", &[ts(TS_2020 + $off * NPD)]), i($m)); } }; }
    // 2020 is leap: cumulative: Feb=31, Mar=60, Apr=91, May=121, Jun=152, Jul=182, Aug=213, Sep=244, Oct=274, Nov=305, Dec=335
    mo!(jan, 0, 1); mo!(feb, 31, 2); mo!(mar, 60, 3); mo!(apr, 91, 4); mo!(may, 121, 5);
    mo!(jun, 152, 6); mo!(jul, 182, 7); mo!(aug, 213, 8); mo!(sep, 244, 9); mo!(oct, 274, 10);
    mo!(nov, 305, 11); mo!(dec, 335, 12);
}

mod month_2021 { use super::*;
    macro_rules! mo { ($n:ident, $off:expr, $m:expr) => { #[test] fn $n() { assert_eq!(ev("extract_month", &[ts(TS_2021 + $off * NPD)]), i($m)); } }; }
    // 2021 not leap: Feb=31, Mar=59, Apr=90, May=120, Jun=151, Jul=181, Aug=212, Sep=243, Oct=273, Nov=304, Dec=334
    mo!(jan, 0, 1); mo!(feb, 31, 2); mo!(mar, 59, 3); mo!(apr, 90, 4); mo!(may, 120, 5);
    mo!(jun, 151, 6); mo!(jul, 181, 7); mo!(aug, 212, 8); mo!(sep, 243, 9); mo!(oct, 273, 10);
    mo!(nov, 304, 11); mo!(dec, 334, 12);
}

mod month_2022 { use super::*;
    macro_rules! mo { ($n:ident, $off:expr, $m:expr) => { #[test] fn $n() { assert_eq!(ev("extract_month", &[ts(TS_2022 + $off * NPD)]), i($m)); } }; }
    mo!(jan, 0, 1); mo!(feb, 31, 2); mo!(mar, 59, 3); mo!(apr, 90, 4); mo!(may, 120, 5);
    mo!(jun, 151, 6); mo!(jul, 181, 7); mo!(aug, 212, 8); mo!(sep, 243, 9); mo!(oct, 273, 10);
    mo!(nov, 304, 11); mo!(dec, 334, 12);
}

mod month_2023 { use super::*;
    macro_rules! mo { ($n:ident, $off:expr, $m:expr) => { #[test] fn $n() { assert_eq!(ev("extract_month", &[ts(TS_2023 + $off * NPD)]), i($m)); } }; }
    mo!(jan, 0, 1); mo!(feb, 31, 2); mo!(mar, 59, 3); mo!(apr, 90, 4); mo!(may, 120, 5);
    mo!(jun, 151, 6); mo!(jul, 181, 7); mo!(aug, 212, 8); mo!(sep, 243, 9); mo!(oct, 273, 10);
    mo!(nov, 304, 11); mo!(dec, 334, 12);
}

mod month_2025 { use super::*;
    macro_rules! mo { ($n:ident, $off:expr, $m:expr) => { #[test] fn $n() { assert_eq!(ev("extract_month", &[ts(TS_2025 + $off * NPD)]), i($m)); } }; }
    mo!(jan, 0, 1); mo!(feb, 31, 2); mo!(mar, 59, 3); mo!(apr, 90, 4); mo!(may, 120, 5);
    mo!(jun, 151, 6); mo!(jul, 181, 7); mo!(aug, 212, 8); mo!(sep, 243, 9); mo!(oct, 273, 10);
    mo!(nov, 304, 11); mo!(dec, 334, 12);
}

// year for each year at various months
mod year_months { use super::*;
    macro_rules! yr { ($n:ident, $base:expr, $off:expr, $y:expr) => { #[test] fn $n() { assert_eq!(ev("extract_year", &[ts($base + $off * NPD)]), i($y)); } }; }
    yr!(y20_jan, TS_2020, 0, 2020); yr!(y20_jun, TS_2020, 152, 2020); yr!(y20_dec, TS_2020, 365, 2020);
    yr!(y21_jan, TS_2021, 0, 2021); yr!(y21_jun, TS_2021, 151, 2021); yr!(y21_dec, TS_2021, 364, 2021);
    yr!(y22_jan, TS_2022, 0, 2022); yr!(y22_jun, TS_2022, 151, 2022); yr!(y22_dec, TS_2022, 364, 2022);
    yr!(y23_jan, TS_2023, 0, 2023); yr!(y23_jun, TS_2023, 151, 2023); yr!(y23_dec, TS_2023, 364, 2023);
    yr!(y25_jan, TS_2025, 0, 2025); yr!(y25_jun, TS_2025, 151, 2025); yr!(y25_dec, TS_2025, 364, 2025);
}

// extract_hour at various day+hour combos
mod hour_combos { use super::*;
    macro_rules! hr { ($n:ident, $d:expr, $h:expr) => { #[test] fn $n() { assert_eq!(ev("extract_hour", &[ts(TS_2024 + $d * NPD + $h * NPH)]), i($h)); } }; }
    hr!(d1h0, 1, 0); hr!(d1h6, 1, 6); hr!(d1h12, 1, 12); hr!(d1h18, 1, 18); hr!(d1h23, 1, 23);
    hr!(d10h0, 10, 0); hr!(d10h6, 10, 6); hr!(d10h12, 10, 12); hr!(d10h18, 10, 18); hr!(d10h23, 10, 23);
    hr!(d50h0, 50, 0); hr!(d50h6, 50, 6); hr!(d50h12, 50, 12); hr!(d50h18, 50, 18); hr!(d50h23, 50, 23);
    hr!(d100h0, 100, 0); hr!(d100h6, 100, 6); hr!(d100h12, 100, 12); hr!(d100h18, 100, 18); hr!(d100h23, 100, 23);
    hr!(d200h0, 200, 0); hr!(d200h6, 200, 6); hr!(d200h12, 200, 12); hr!(d200h18, 200, 18); hr!(d200h23, 200, 23);
    hr!(d300h0, 300, 0); hr!(d300h6, 300, 6); hr!(d300h12, 300, 12); hr!(d300h18, 300, 18); hr!(d300h23, 300, 23);
    hr!(d365h0, 365, 0); hr!(d365h12, 365, 12);
}

// extract_day for first of each month across years
mod day_first { use super::*;
    macro_rules! d1 { ($n:ident, $base:expr, $off:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts($base + $off * NPD)]), i(1)); } }; }
    d1!(y20_jan, TS_2020, 0); d1!(y20_feb, TS_2020, 31); d1!(y20_mar, TS_2020, 60);
    d1!(y20_apr, TS_2020, 91); d1!(y20_may, TS_2020, 121); d1!(y20_jun, TS_2020, 152);
    d1!(y20_jul, TS_2020, 182); d1!(y20_aug, TS_2020, 213); d1!(y20_sep, TS_2020, 244);
    d1!(y20_oct, TS_2020, 274); d1!(y20_nov, TS_2020, 305); d1!(y20_dec, TS_2020, 335);
    d1!(y21_jan, TS_2021, 0); d1!(y21_feb, TS_2021, 31); d1!(y21_mar, TS_2021, 59);
    d1!(y21_apr, TS_2021, 90); d1!(y21_may, TS_2021, 120); d1!(y21_jun, TS_2021, 151);
    d1!(y22_jan, TS_2022, 0); d1!(y22_feb, TS_2022, 31); d1!(y22_mar, TS_2022, 59);
    d1!(y22_apr, TS_2022, 90); d1!(y22_may, TS_2022, 120); d1!(y22_jun, TS_2022, 151);
    d1!(y23_jan, TS_2023, 0); d1!(y23_feb, TS_2023, 31); d1!(y23_mar, TS_2023, 59);
    d1!(y24_jan, TS_2024, 0); d1!(y24_feb, TS_2024, 31); d1!(y24_mar, TS_2024, 60);
    d1!(y25_jan, TS_2025, 0); d1!(y25_feb, TS_2025, 31); d1!(y25_mar, TS_2025, 59);
}

// Aliases
mod aliases { use super::*;
    #[test] fn year_of() { assert_eq!(ev("year_of", &[ts(TS_2024)]), i(2024)); }
    #[test] fn month_of_year() { assert_eq!(ev("month_of_year", &[ts(TS_2024)]), i(1)); }
    #[test] fn day_of_month() { assert_eq!(ev("day_of_month", &[ts(TS_2024)]), i(1)); }
    #[test] fn year_of_2020() { assert_eq!(ev("year_of", &[ts(TS_2020)]), i(2020)); }
    #[test] fn month_of_year_jun() { assert_eq!(ev("month_of_year", &[ts(TS_2024 + 152 * NPD)]), i(6)); }
    #[test] fn day_of_month_15() { assert_eq!(ev("day_of_month", &[ts(TS_2024 + 14 * NPD)]), i(15)); }
}

// extract_day for Apr-Dec in 2020 (leap year)
mod day_2020_apr { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2020 + $off * NPD)]), i($d)); } }; }
    // Apr 2020: offset 91
    dy!(d01, 91, 1); dy!(d10, 100, 10); dy!(d15, 105, 15); dy!(d20, 110, 20); dy!(d30, 120, 30);
    // May 2020: offset 121
    dy!(may01, 121, 1); dy!(may15, 135, 15); dy!(may31, 151, 31);
    // Jun 2020: offset 152
    dy!(jun01, 152, 1); dy!(jun15, 166, 15); dy!(jun30, 181, 30);
    // Jul 2020: offset 182
    dy!(jul01, 182, 1); dy!(jul15, 196, 15); dy!(jul31, 212, 31);
    // Aug 2020: offset 213
    dy!(aug01, 213, 1); dy!(aug15, 227, 15); dy!(aug31, 243, 31);
    // Sep 2020: offset 244
    dy!(sep01, 244, 1); dy!(sep15, 258, 15); dy!(sep30, 273, 30);
    // Oct 2020: offset 274
    dy!(oct01, 274, 1); dy!(oct15, 288, 15); dy!(oct31, 304, 31);
    // Nov 2020: offset 305
    dy!(nov01, 305, 1); dy!(nov15, 319, 15); dy!(nov30, 334, 30);
    // Dec 2020: offset 335
    dy!(dec01, 335, 1); dy!(dec15, 349, 15); dy!(dec31, 365, 31);
}

// extract_hour for each hour at different dates in 2020
mod hour_2020 { use super::*;
    macro_rules! hr { ($n:ident, $d:expr, $h:expr) => { #[test] fn $n() { assert_eq!(ev("extract_hour", &[ts(TS_2020 + $d * NPD + $h * NPH)]), i($h)); } }; }
    hr!(d0h0, 0, 0); hr!(d0h6, 0, 6); hr!(d0h12, 0, 12); hr!(d0h18, 0, 18); hr!(d0h23, 0, 23);
    hr!(d30h0, 30, 0); hr!(d30h6, 30, 6); hr!(d30h12, 30, 12); hr!(d30h18, 30, 18); hr!(d30h23, 30, 23);
    hr!(d60h0, 60, 0); hr!(d60h12, 60, 12); hr!(d60h23, 60, 23);
    hr!(d100h0, 100, 0); hr!(d100h12, 100, 12); hr!(d100h23, 100, 23);
    hr!(d200h0, 200, 0); hr!(d200h12, 200, 12); hr!(d200h23, 200, 23);
    hr!(d300h0, 300, 0); hr!(d300h12, 300, 12); hr!(d300h23, 300, 23);
}

// extract_year at various 2021-2023 offsets
mod year_extra { use super::*;
    macro_rules! yr { ($n:ident, $base:expr, $off:expr, $y:expr) => { #[test] fn $n() { assert_eq!(ev("extract_year", &[ts($base + $off * NPD)]), i($y)); } }; }
    yr!(y21_d100, TS_2021, 100, 2021); yr!(y21_d200, TS_2021, 200, 2021);
    yr!(y21_d300, TS_2021, 300, 2021); yr!(y21_d364, TS_2021, 364, 2021);
    yr!(y22_d100, TS_2022, 100, 2022); yr!(y22_d200, TS_2022, 200, 2022);
    yr!(y22_d300, TS_2022, 300, 2022); yr!(y22_d364, TS_2022, 364, 2022);
    yr!(y23_d100, TS_2023, 100, 2023); yr!(y23_d200, TS_2023, 200, 2023);
    yr!(y23_d300, TS_2023, 300, 2023); yr!(y23_d364, TS_2023, 364, 2023);
    yr!(y20_d100, TS_2020, 100, 2020); yr!(y20_d200, TS_2020, 200, 2020);
    yr!(y20_d300, TS_2020, 300, 2020); yr!(y20_d365, TS_2020, 365, 2020);
    yr!(y25_d100, TS_2025, 100, 2025); yr!(y25_d200, TS_2025, 200, 2025);
    yr!(y25_d300, TS_2025, 300, 2025); yr!(y25_d364, TS_2025, 364, 2025);
}

// extract_day for Jan in 2021-2023
mod day_jan_multi { use super::*;
    macro_rules! d { ($n:ident, $base:expr, $off:expr, $day:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts($base + $off * NPD)]), i($day)); } }; }
    d!(y21_d1, TS_2021, 0, 1); d!(y21_d5, TS_2021, 4, 5); d!(y21_d10, TS_2021, 9, 10);
    d!(y21_d15, TS_2021, 14, 15); d!(y21_d20, TS_2021, 19, 20); d!(y21_d25, TS_2021, 24, 25);
    d!(y21_d31, TS_2021, 30, 31);
    d!(y22_d1, TS_2022, 0, 1); d!(y22_d5, TS_2022, 4, 5); d!(y22_d10, TS_2022, 9, 10);
    d!(y22_d15, TS_2022, 14, 15); d!(y22_d20, TS_2022, 19, 20); d!(y22_d25, TS_2022, 24, 25);
    d!(y22_d31, TS_2022, 30, 31);
    d!(y23_d1, TS_2023, 0, 1); d!(y23_d5, TS_2023, 4, 5); d!(y23_d10, TS_2023, 9, 10);
    d!(y23_d15, TS_2023, 14, 15); d!(y23_d20, TS_2023, 19, 20); d!(y23_d25, TS_2023, 24, 25);
    d!(y23_d31, TS_2023, 30, 31);
    d!(y25_d1, TS_2025, 0, 1); d!(y25_d5, TS_2025, 4, 5); d!(y25_d10, TS_2025, 9, 10);
    d!(y25_d15, TS_2025, 14, 15); d!(y25_d20, TS_2025, 19, 20); d!(y25_d25, TS_2025, 24, 25);
    d!(y25_d31, TS_2025, 30, 31);
}

// extract_month for months in 2025
mod month_2025_extra { use super::*;
    macro_rules! mo { ($n:ident, $off:expr, $m:expr) => { #[test] fn $n() { assert_eq!(ev("extract_month", &[ts(TS_2025 + $off * NPD)]), i($m)); } }; }
    mo!(jan5, 4, 1); mo!(jan15, 14, 1); mo!(jan25, 24, 1);
    mo!(feb5, 35, 2); mo!(feb15, 45, 2); mo!(feb25, 55, 2);
    mo!(mar5, 63, 3); mo!(mar15, 73, 3); mo!(mar25, 83, 3);
    mo!(apr5, 94, 4); mo!(apr15, 104, 4); mo!(apr25, 114, 4);
    mo!(may5, 124, 5); mo!(may15, 134, 5); mo!(may25, 144, 5);
    mo!(jun5, 155, 6); mo!(jun15, 165, 6); mo!(jun25, 175, 6);
    mo!(jul5, 185, 7); mo!(jul15, 195, 7); mo!(jul25, 205, 7);
    mo!(aug5, 216, 8); mo!(aug15, 226, 8); mo!(aug25, 236, 8);
    mo!(sep5, 247, 9); mo!(sep15, 257, 9); mo!(sep25, 267, 9);
    mo!(oct5, 277, 10); mo!(oct15, 287, 10); mo!(oct25, 297, 10);
    mo!(nov5, 308, 11); mo!(nov15, 318, 11); mo!(nov25, 328, 11);
    mo!(dec5, 338, 12); mo!(dec15, 348, 12); mo!(dec25, 358, 12);
}

// extract_hour for 2021 at various offsets
mod hour_2021 { use super::*;
    macro_rules! hr { ($n:ident, $d:expr, $h:expr) => { #[test] fn $n() { assert_eq!(ev("extract_hour", &[ts(TS_2021 + $d * NPD + $h * NPH)]), i($h)); } }; }
    hr!(d0h0, 0, 0); hr!(d0h1, 0, 1); hr!(d0h2, 0, 2); hr!(d0h3, 0, 3);
    hr!(d0h4, 0, 4); hr!(d0h5, 0, 5); hr!(d0h6, 0, 6); hr!(d0h7, 0, 7);
    hr!(d0h8, 0, 8); hr!(d0h9, 0, 9); hr!(d0h10, 0, 10); hr!(d0h11, 0, 11);
    hr!(d0h12, 0, 12); hr!(d0h13, 0, 13); hr!(d0h14, 0, 14); hr!(d0h15, 0, 15);
    hr!(d0h16, 0, 16); hr!(d0h17, 0, 17); hr!(d0h18, 0, 18); hr!(d0h19, 0, 19);
    hr!(d0h20, 0, 20); hr!(d0h21, 0, 21); hr!(d0h22, 0, 22); hr!(d0h23, 0, 23);
    hr!(d30h0, 30, 0); hr!(d30h6, 30, 6); hr!(d30h12, 30, 12); hr!(d30h18, 30, 18);
    hr!(d60h0, 60, 0); hr!(d60h12, 60, 12); hr!(d90h0, 90, 0); hr!(d90h12, 90, 12);
    hr!(d120h0, 120, 0); hr!(d120h12, 120, 12); hr!(d180h0, 180, 0); hr!(d180h12, 180, 12);
    hr!(d240h0, 240, 0); hr!(d240h12, 240, 12); hr!(d300h0, 300, 0); hr!(d300h12, 300, 12);
    hr!(d364h0, 364, 0); hr!(d364h23, 364, 23);
}

// extract_day for 2022 across months
mod day_2022 { use super::*;
    macro_rules! dy { ($n:ident, $off:expr, $d:expr) => { #[test] fn $n() { assert_eq!(ev("extract_day", &[ts(TS_2022 + $off * NPD)]), i($d)); } }; }
    // Jan
    dy!(jan01, 0, 1); dy!(jan05, 4, 5); dy!(jan10, 9, 10); dy!(jan15, 14, 15);
    dy!(jan20, 19, 20); dy!(jan25, 24, 25); dy!(jan31, 30, 31);
    // Feb (not leap, 28 days)
    dy!(feb01, 31, 1); dy!(feb05, 35, 5); dy!(feb10, 40, 10); dy!(feb15, 45, 15);
    dy!(feb20, 50, 20); dy!(feb25, 55, 25); dy!(feb28, 58, 28);
    // Mar
    dy!(mar01, 59, 1); dy!(mar05, 63, 5); dy!(mar10, 68, 10); dy!(mar15, 73, 15);
    dy!(mar20, 78, 20); dy!(mar25, 83, 25); dy!(mar31, 89, 31);
    // Apr
    dy!(apr01, 90, 1); dy!(apr10, 99, 10); dy!(apr15, 104, 15); dy!(apr30, 119, 30);
    // May
    dy!(may01, 120, 1); dy!(may15, 134, 15); dy!(may31, 150, 31);
    // Jun
    dy!(jun01, 151, 1); dy!(jun15, 165, 15); dy!(jun30, 180, 30);
    // Jul
    dy!(jul01, 181, 1); dy!(jul15, 195, 15); dy!(jul31, 211, 31);
}

// extract_year consistency for epoch timestamps
mod year_epoch { use super::*;
    #[test] fn epoch_0() { assert_eq!(ev("extract_year", &[ts(0)]), i(1970)); }
    #[test] fn epoch_1d() { assert_eq!(ev("extract_year", &[ts(NPD)]), i(1970)); }
    #[test] fn epoch_30d() { assert_eq!(ev("extract_year", &[ts(30 * NPD)]), i(1970)); }
    #[test] fn epoch_364d() { assert_eq!(ev("extract_year", &[ts(364 * NPD)]), i(1970)); }
    #[test] fn epoch_365d() { assert_eq!(ev("extract_year", &[ts(365 * NPD)]), i(1971)); }
    #[test] fn epoch_730d() { assert_eq!(ev("extract_year", &[ts(730 * NPD)]), i(1972)); }
    #[test] fn epoch_neg1d() { assert_eq!(ev("extract_year", &[ts(-NPD)]), i(1969)); }
}

// extract_month for epoch
mod month_epoch { use super::*;
    #[test] fn epoch_jan() { assert_eq!(ev("extract_month", &[ts(0)]), i(1)); }
    #[test] fn epoch_feb() { assert_eq!(ev("extract_month", &[ts(31 * NPD)]), i(2)); }
    #[test] fn epoch_mar() { assert_eq!(ev("extract_month", &[ts(59 * NPD)]), i(3)); }
    #[test] fn epoch_apr() { assert_eq!(ev("extract_month", &[ts(90 * NPD)]), i(4)); }
    #[test] fn epoch_may() { assert_eq!(ev("extract_month", &[ts(120 * NPD)]), i(5)); }
    #[test] fn epoch_jun() { assert_eq!(ev("extract_month", &[ts(151 * NPD)]), i(6)); }
    #[test] fn epoch_jul() { assert_eq!(ev("extract_month", &[ts(181 * NPD)]), i(7)); }
    #[test] fn epoch_aug() { assert_eq!(ev("extract_month", &[ts(212 * NPD)]), i(8)); }
    #[test] fn epoch_sep() { assert_eq!(ev("extract_month", &[ts(243 * NPD)]), i(9)); }
    #[test] fn epoch_oct() { assert_eq!(ev("extract_month", &[ts(273 * NPD)]), i(10)); }
    #[test] fn epoch_nov() { assert_eq!(ev("extract_month", &[ts(304 * NPD)]), i(11)); }
    #[test] fn epoch_dec() { assert_eq!(ev("extract_month", &[ts(334 * NPD)]), i(12)); }
}

// extract_day at epoch
mod day_epoch { use super::*;
    #[test] fn epoch_d1() { assert_eq!(ev("extract_day", &[ts(0)]), i(1)); }
    #[test] fn epoch_d2() { assert_eq!(ev("extract_day", &[ts(NPD)]), i(2)); }
    #[test] fn epoch_d15() { assert_eq!(ev("extract_day", &[ts(14 * NPD)]), i(15)); }
    #[test] fn epoch_d31() { assert_eq!(ev("extract_day", &[ts(30 * NPD)]), i(31)); }
}

// extract_hour at epoch
mod hour_epoch { use super::*;
    macro_rules! hr { ($n:ident, $h:expr) => { #[test] fn $n() { assert_eq!(ev("extract_hour", &[ts($h * NPH)]), i($h)); } }; }
    hr!(h0, 0); hr!(h1, 1); hr!(h2, 2); hr!(h3, 3); hr!(h4, 4); hr!(h5, 5);
    hr!(h6, 6); hr!(h7, 7); hr!(h8, 8); hr!(h9, 9); hr!(h10, 10); hr!(h11, 11);
    hr!(h12, 12); hr!(h13, 13); hr!(h14, 14); hr!(h15, 15); hr!(h16, 16); hr!(h17, 17);
    hr!(h18, 18); hr!(h19, 19); hr!(h20, 20); hr!(h21, 21); hr!(h22, 22); hr!(h23, 23);
}
