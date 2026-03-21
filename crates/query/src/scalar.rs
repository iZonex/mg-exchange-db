//! Scalar functions: string, math, date/time, and conditional functions.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use regex::Regex;

use crate::plan::Value;

/// Trait for scalar functions that take arguments and produce a single value.
pub trait ScalarFunction: Send + Sync {
    /// Evaluate the function with the given arguments.
    fn evaluate(&self, args: &[Value]) -> Result<Value, String>;

    /// Return the minimum number of arguments this function expects.
    fn min_args(&self) -> usize {
        0
    }

    /// Return the maximum number of arguments (usize::MAX for variadic).
    fn max_args(&self) -> usize {
        self.min_args()
    }
}

/// Registry of scalar functions, looked up by name.
pub struct ScalarRegistry {
    functions: HashMap<String, Box<dyn ScalarFunction>>,
}

impl ScalarRegistry {
    /// Create a new registry with all built-in scalar functions registered.
    pub fn new() -> Self {
        let mut reg = Self {
            functions: HashMap::new(),
        };
        reg.register_all();
        reg
    }

    /// Look up a function by name (case-insensitive).
    pub fn get(&self, name: &str) -> Option<&dyn ScalarFunction> {
        self.functions.get(&name.to_ascii_lowercase()).map(|b| b.as_ref())
    }

    fn register(&mut self, name: &str, f: Box<dyn ScalarFunction>) {
        self.functions.insert(name.to_ascii_lowercase(), f);
    }

    /// Public registration method for use by external modules (e.g. `casts`).
    pub fn register_public(&mut self, name: &str, f: Box<dyn ScalarFunction>) {
        self.functions.insert(name.to_ascii_lowercase(), f);
    }

    fn register_all(&mut self) {
        // String functions
        self.register("length", Box::new(LengthFn));
        self.register("upper", Box::new(UpperFn));
        self.register("lower", Box::new(LowerFn));
        self.register("trim", Box::new(TrimFn));
        self.register("ltrim", Box::new(LtrimFn));
        self.register("rtrim", Box::new(RtrimFn));
        self.register("substring", Box::new(SubstringFn));
        self.register("concat", Box::new(ConcatFn));
        self.register("replace", Box::new(ReplaceFn));
        self.register("starts_with", Box::new(StartsWithFn));
        self.register("ends_with", Box::new(EndsWithFn));
        self.register("contains", Box::new(ContainsFn));
        self.register("reverse", Box::new(ReverseFn));
        self.register("repeat", Box::new(RepeatFn));
        self.register("left", Box::new(LeftFn));
        self.register("right", Box::new(RightFn));

        // Math functions
        self.register("abs", Box::new(AbsFn));
        self.register("round", Box::new(RoundFn));
        self.register("floor", Box::new(FloorFn));
        self.register("ceil", Box::new(CeilFn));
        self.register("sqrt", Box::new(SqrtFn));
        self.register("pow", Box::new(PowFn));
        self.register("log", Box::new(LogFn));
        self.register("log2", Box::new(Log2Fn));
        self.register("log10", Box::new(Log10Fn));
        self.register("exp", Box::new(ExpFn));
        self.register("sin", Box::new(SinFn));
        self.register("cos", Box::new(CosFn));
        self.register("tan", Box::new(TanFn));
        self.register("mod", Box::new(ModFn));
        self.register("sign", Box::new(SignFn));
        self.register("pi", Box::new(PiFn));
        self.register("random", Box::new(RandomFn));

        // Date/time functions
        self.register("now", Box::new(NowFn));
        self.register("to_timestamp", Box::new(ToTimestampFn));
        self.register("extract_year", Box::new(ExtractYearFn));
        self.register("extract_month", Box::new(ExtractMonthFn));
        self.register("extract_day", Box::new(ExtractDayFn));
        self.register("extract_hour", Box::new(ExtractHourFn));
        self.register("date_trunc", Box::new(DateTruncFn));
        self.register("date_diff", Box::new(DateDiffFn));
        self.register("timestamp_add", Box::new(TimestampAddFn));
        self.register("epoch_nanos", Box::new(EpochNanosFn));

        // Conditional functions
        self.register("coalesce", Box::new(CoalesceFn));
        self.register("nullif", Box::new(NullIfFn));
        self.register("greatest", Box::new(GreatestFn));
        self.register("least", Box::new(LeastFn));
        self.register("if_null", Box::new(IfNullFn));

        // Additional string functions
        self.register("lpad", Box::new(LpadFn));
        self.register("rpad", Box::new(RpadFn));
        self.register("split_part", Box::new(SplitPartFn));
        self.register("regexp_match", Box::new(RegexpMatchFn));
        self.register("regexp_match_ci", Box::new(RegexpMatchCiFn));
        self.register("regexp_replace", Box::new(RegexpReplaceFn));
        self.register("regexp_extract", Box::new(RegexpExtractFn));
        self.register("md5", Box::new(Md5Fn));
        self.register("sha256", Box::new(Sha256Fn));
        self.register("to_char", Box::new(ToCharFn));
        self.register("char_length", Box::new(CharLengthFn));
        self.register("position", Box::new(PositionFn));
        self.register("overlay", Box::new(OverlayFn));
        self.register("translate", Box::new(TranslateFn));
        self.register("initcap", Box::new(InitcapFn));
        self.register("encode", Box::new(EncodeFn));
        self.register("decode", Box::new(DecodeFn));
        self.register("quote_ident", Box::new(QuoteIdentFn));
        self.register("quote_literal", Box::new(QuoteLiteralFn));
        self.register("format", Box::new(FormatFn));
        self.register("ascii", Box::new(AsciiFn));
        self.register("chr", Box::new(ChrFn));

        // Type casting functions
        self.register("cast_int", Box::new(CastIntFn));
        self.register("to_int", Box::new(CastIntFn));
        self.register("cast_float", Box::new(CastFloatFn));
        self.register("to_float", Box::new(CastFloatFn));
        self.register("cast_str", Box::new(CastStrFn));
        self.register("to_str", Box::new(CastStrFn));
        self.register("cast_bool", Box::new(CastBoolFn));
        self.register("cast_timestamp", Box::new(CastTimestampFn));
        self.register("to_date", Box::new(ToDateFn));
        self.register("to_number", Box::new(ToNumberFn));
        self.register("typeof", Box::new(TypeOfFn));
        self.register("is_null", Box::new(IsNullFn));
        self.register("is_not_null", Box::new(IsNotNullFn));
        self.register("nullif_zero", Box::new(NullIfZeroFn));

        // Additional math functions
        self.register("degrees", Box::new(DegreesFn));
        self.register("radians", Box::new(RadiansFn));
        self.register("atan2", Box::new(Atan2Fn));
        self.register("asin", Box::new(AsinFn));
        self.register("acos", Box::new(AcosFn));
        self.register("atan", Box::new(AtanFn));
        self.register("sinh", Box::new(SinhFn));
        self.register("cosh", Box::new(CoshFn));
        self.register("tanh", Box::new(TanhFn));
        self.register("ln", Box::new(LogFn)); // alias for log
        self.register("cbrt", Box::new(CbrtFn));
        self.register("factorial", Box::new(FactorialFn));
        self.register("gcd", Box::new(GcdFn));
        self.register("lcm", Box::new(LcmFn));
        self.register("bit_and", Box::new(BitAndFn));
        self.register("bit_or", Box::new(BitOrFn));
        self.register("bit_xor", Box::new(BitXorFn));
        self.register("bit_not", Box::new(BitNotFn));
        self.register("bit_shift_left", Box::new(BitShiftLeftFn));
        self.register("bit_shift_right", Box::new(BitShiftRightFn));
        self.register("trunc", Box::new(TruncFn));
        self.register("div", Box::new(DivFn));
        self.register("width_bucket", Box::new(WidthBucketFn));

        // Additional date/time functions
        self.register("extract_minute", Box::new(ExtractMinuteFn));
        self.register("extract_second", Box::new(ExtractSecondFn));
        self.register("extract_week", Box::new(ExtractWeekFn));
        self.register("extract_day_of_week", Box::new(ExtractDayOfWeekFn));
        self.register("extract_day_of_year", Box::new(ExtractDayOfYearFn));
        self.register("extract_quarter", Box::new(ExtractQuarterFn));
        self.register("to_str_timestamp", Box::new(ToStrTimestampFn));
        self.register("date_part", Box::new(DatePartFn));
        self.register("age", Box::new(AgeFn));
        self.register("make_timestamp", Box::new(MakeTimestampFn));
        self.register("current_timestamp", Box::new(NowFn)); // alias for now
        self.register("timestamp_floor", Box::new(DateTruncFn)); // alias for date_trunc
        self.register("timestamp_ceil", Box::new(TimestampCeilFn));
        self.register("interval_to_nanos", Box::new(IntervalToNanosFn));
        self.register("days_in_month_fn", Box::new(DaysInMonthFn));
        self.register("is_leap_year_fn", Box::new(IsLeapYearFn));
        self.register("first_of_month", Box::new(FirstOfMonthFn));
        self.register("last_of_month", Box::new(LastOfMonthFn));
        self.register("next_day", Box::new(NextDayFn));
        self.register("months_between", Box::new(MonthsBetweenFn));
        self.register("years_between", Box::new(YearsBetweenFn));

        // Cast functions
        self.register("cast_to_int", Box::new(CastToIntFn));
        self.register("cast_to_float", Box::new(CastToFloatFn));
        self.register("cast_to_str", Box::new(CastToStrFn));
        self.register("cast_to_timestamp", Box::new(CastToTimestampFn));

        // Random / testing functions
        self.register("rnd_int", Box::new(RndIntFn));
        self.register("rnd_long", Box::new(RndIntFn));
        self.register("rnd_double", Box::new(RndDoubleFn));
        self.register("rnd_float", Box::new(RndFloatFn));
        self.register("rnd_str", Box::new(RndStrFn));
        self.register("rnd_symbol", Box::new(RndStrFn));
        self.register("rnd_boolean", Box::new(RndBooleanFn));
        self.register("rnd_timestamp", Box::new(RndTimestampFn));
        self.register("rnd_uuid4", Box::new(RndUuid4Fn));

        // ── New date/time functions ─────────────────────────────────
        self.register("to_timezone", Box::new(ToTimezoneFn));
        self.register("from_utc", Box::new(FromUtcFn));
        self.register("to_utc", Box::new(ToUtcFn));
        self.register("date_format", Box::new(DateFormatFn));
        self.register("day_of_month", Box::new(ExtractDayFn)); // alias
        self.register("month_of_year", Box::new(ExtractMonthFn)); // alias
        self.register("year_of", Box::new(ExtractYearFn)); // alias
        self.register("week_of_year", Box::new(ExtractWeekFn)); // alias
        self.register("quarter_of_year", Box::new(ExtractQuarterFn)); // alias
        self.register("epoch_seconds", Box::new(EpochSecondsFn));
        self.register("epoch_millis", Box::new(EpochMillisFn));
        self.register("epoch_micros", Box::new(EpochMicrosFn));
        self.register("systimestamp", Box::new(NowFn)); // alias for now
        self.register("sysdate", Box::new(NowFn)); // alias for now
        self.register("is_weekend", Box::new(IsWeekendFn));
        self.register("is_business_day", Box::new(IsBusinessDayFn));
        self.register("timestamp_sequence", Box::new(TimestampSequenceFn));

        // ── New string functions ────────────────────────────────────
        self.register("to_lowercase", Box::new(LowerFn)); // alias
        self.register("to_uppercase", Box::new(UpperFn)); // alias
        self.register("str_pos", Box::new(PositionFn)); // alias
        self.register("char_at", Box::new(CharAtFn));
        self.register("hex", Box::new(HexFn));
        self.register("unhex", Box::new(UnhexFn));
        self.register("url_encode", Box::new(UrlEncodeFn));
        self.register("url_decode", Box::new(UrlDecodeFn));
        self.register("json_extract", Box::new(JsonExtractFn));
        self.register("json_array_length", Box::new(JsonArrayLengthFn));
        self.register("regexp_count", Box::new(RegexpCountFn));
        self.register("regexp_split_to_array", Box::new(RegexpSplitToArrayFn));
        self.register("string_to_array", Box::new(StringToArrayFn));
        self.register("array_to_string", Box::new(ArrayToStringFn));

        // ── New math functions ──────────────────────────────────────
        self.register("clamp", Box::new(ClampFn));
        self.register("lerp", Box::new(LerpFn));
        self.register("map_range", Box::new(MapRangeFn));
        self.register("is_finite", Box::new(IsFiniteFn));
        self.register("is_nan", Box::new(IsNanFn));
        self.register("is_inf", Box::new(IsInfFn));
        self.register("fma", Box::new(FmaFn));
        self.register("hypot", Box::new(HypotFn));
        self.register("copysign", Box::new(CopysignFn));
        self.register("next_power_of_two", Box::new(NextPowerOfTwoFn));

        // ── Utility functions ───────────────────────────────────────
        self.register("sizeof", Box::new(SizeofFn));
        self.register("version", Box::new(VersionFn));
        self.register("pg_typeof", Box::new(PgTypeofFn));
        self.register("generate_uid", Box::new(RndUuid4Fn)); // alias
        self.register("hash", Box::new(HashFn));
        self.register("murmur3", Box::new(Murmur3Fn));
        self.register("crc32", Box::new(Crc32Fn));
        self.register("to_json", Box::new(ToJsonFn));
        self.register("table_name", Box::new(TableNameFn));

        // ── Extra aliases for QuestDB compat ────────────────────────
        self.register("ceiling", Box::new(CeilFn));
        self.register("power", Box::new(PowFn));
        self.register("substr", Box::new(SubstringFn));
        self.register("len", Box::new(LengthFn));
        self.register("string_length", Box::new(LengthFn));
        self.register("to_long", Box::new(CastIntFn));
        self.register("to_double", Box::new(CastFloatFn));
        self.register("to_string", Box::new(CastStrFn));
        self.register("to_short", Box::new(CastIntFn));
        self.register("to_byte", Box::new(CastIntFn));
        self.register("to_boolean", Box::new(CastBoolFn));
        self.register("strftime", Box::new(DateFormatFn));
        self.register("date_add", Box::new(TimestampAddFn));
        self.register("dateadd", Box::new(TimestampAddFn));
        self.register("datediff", Box::new(DateDiffFn));
        self.register("now_utc", Box::new(NowFn));
        self.register("current_date", Box::new(NowFn));
        self.register("localtime", Box::new(NowFn));
        self.register("localtimestamp", Box::new(NowFn));
        self.register("timestamp_diff", Box::new(DateDiffFn));
        self.register("e", Box::new(EConstFn));
        self.register("tau", Box::new(TauConstFn));
        self.register("infinity", Box::new(InfinityFn));
        self.register("nan", Box::new(NanFn));

        // Additional conversion/compat aliases
        self.register("from_unixtime", Box::new(ToTimestampFn));
        self.register("unix_timestamp", Box::new(EpochSecondsFn));
        self.register("to_unix_timestamp", Box::new(EpochSecondsFn));
        self.register("char", Box::new(ChrFn));
        self.register("ifnull", Box::new(IfNullFn));
        self.register("nvl", Box::new(IfNullFn));
        self.register("nvl2", Box::new(Nvl2Fn));
        self.register("decode_fn", Box::new(DecodeCaseFn));
        self.register("iif", Box::new(IifFn));
        self.register("case_when", Box::new(IifFn));
        self.register("switch", Box::new(SwitchFn));

        // More math aliases/functions
        self.register("log_base", Box::new(LogBaseFn));
        self.register("square", Box::new(SquareFn));
        self.register("remainder", Box::new(ModFn));
        self.register("truncate", Box::new(TruncFn));
        self.register("ceiling_fn", Box::new(CeilFn));
        self.register("modulo", Box::new(ModFn));
        self.register("negate", Box::new(NegateFn));
        self.register("reciprocal", Box::new(ReciprocalFn));

        // Crypto/hash aliases
        self.register("hash_code", Box::new(HashFn));
        self.register("fnv1a", Box::new(Fnv1aFn));

        // More string functions
        self.register("strcmp", Box::new(StrcmpFn));
        self.register("soundex", Box::new(SoundexFn));
        self.register("space", Box::new(SpaceFn));
        self.register("to_base64", Box::new(ToBase64Fn));
        self.register("from_base64", Box::new(FromBase64Fn));
        self.register("word_count", Box::new(WordCountFn));
        self.register("camel_case", Box::new(CamelCaseFn));
        self.register("snake_case", Box::new(SnakeCaseFn));
        self.register("title_case", Box::new(InitcapFn));
        self.register("squeeze", Box::new(SqueezeFn));
        self.register("count_char", Box::new(CountCharFn));

        // Date boundary functions
        self.register("start_of_year", Box::new(StartOfYearFn));
        self.register("end_of_year", Box::new(EndOfYearFn));
        self.register("start_of_quarter", Box::new(StartOfQuarterFn));
        self.register("start_of_week", Box::new(StartOfWeekFn));
        self.register("day_of_year", Box::new(ExtractDayOfYearFn));
        self.register("day_of_week", Box::new(ExtractDayOfWeekFn));
        self.register("hour_of_day", Box::new(ExtractHourFn));
        self.register("minute_of_hour", Box::new(ExtractMinuteFn));
        self.register("second_of_minute", Box::new(ExtractSecondFn));

        // Numeric predicates
        self.register("is_positive", Box::new(IsPositiveFn));
        self.register("is_negative", Box::new(IsNegativeFn));
        self.register("is_zero", Box::new(IsZeroFn));
        self.register("is_even", Box::new(IsEvenFn));
        self.register("is_odd", Box::new(IsOddFn));
        self.register("between", Box::new(BetweenFn));

        // More misc
        self.register("row_number", Box::new(RowNumberFn));
        self.register("hash_combine", Box::new(HashCombineFn));
        self.register("min_of", Box::new(LeastFn));
        self.register("max_of", Box::new(GreatestFn));
        self.register("zeroifnull", Box::new(ZeroIfNullFn));
        self.register("nullifempty", Box::new(NullIfEmptyFn));

        // Additional QuestDB/PostgreSQL compat aliases
        self.register("string_agg_fn", Box::new(ConcatFn));
        self.register("concat_ws", Box::new(ConcatWsFn));
        self.register("bit_count", Box::new(BitCountFn));
        self.register("popcount", Box::new(BitCountFn));
        self.register("leading_zeros", Box::new(LeadingZerosFn));
        self.register("trailing_zeros", Box::new(TrailingZerosFn));
        self.register("byte_length", Box::new(ByteLengthFn));
        self.register("octet_length", Box::new(ByteLengthFn));
        self.register("bit_length", Box::new(BitLengthFn));
        self.register("to_hex", Box::new(HexFn));
        self.register("from_hex", Box::new(UnhexFn));
        self.register("digest", Box::new(Md5Fn));
        self.register("coalesce_str", Box::new(CoalesceFn));
        self.register("greatest_fn", Box::new(GreatestFn));
        self.register("least_fn", Box::new(LeastFn));
        self.register("now_fn", Box::new(NowFn));
        self.register("current_time", Box::new(NowFn));
        self.register("today", Box::new(NowFn));
        self.register("yesterday", Box::new(YesterdayFn));
        self.register("tomorrow", Box::new(TomorrowFn));
        self.register("first_day_of_week", Box::new(StartOfWeekFn));
        self.register("first_day_of_month", Box::new(FirstOfMonthFn));
        self.register("last_day_of_month", Box::new(LastOfMonthFn));
        self.register("first_day_of_quarter", Box::new(StartOfQuarterFn));
        self.register("first_day_of_year", Box::new(StartOfYearFn));
        self.register("last_day_of_year", Box::new(EndOfYearFn));
        self.register("timestamp_to_str", Box::new(DateFormatFn));
        self.register("format_timestamp", Box::new(DateFormatFn));
        self.register("str_to_timestamp", Box::new(ToTimestampFn));
        self.register("parse_timestamp", Box::new(ToTimestampFn));
        self.register("safe_cast_int", Box::new(SafeCastIntFn));
        self.register("safe_cast_float", Box::new(SafeCastFloatFn));
        self.register("try_cast_int", Box::new(SafeCastIntFn));
        self.register("try_cast_float", Box::new(SafeCastFloatFn));
        self.register("abs_diff", Box::new(AbsDiffFn));
        self.register("signum", Box::new(SignFn));
        self.register("rand", Box::new(RandomFn));
        self.register("random_int", Box::new(RndIntFn));
        self.register("uuid", Box::new(RndUuid4Fn));
        self.register("uuid4", Box::new(RndUuid4Fn));
        self.register("newid", Box::new(RndUuid4Fn));
        self.register("current_schema", Box::new(CurrentSchemaFn));
        self.register("current_database", Box::new(CurrentDatabaseFn));
        self.register("current_user", Box::new(CurrentUserFn));

        // ── Additional QuestDB / PostgreSQL compat functions ──────────
        self.register("rnd_date", Box::new(RndTimestampFn)); // alias
        self.register("rnd_byte", Box::new(RndIntFn));
        self.register("rnd_short", Box::new(RndIntFn));
        self.register("rnd_bin", Box::new(RndStrFn));
        self.register("rnd_char", Box::new(RndStrFn));
        self.register("rnd_long256", Box::new(RndIntFn));
        self.register("rnd_geohash", Box::new(RndIntFn));
        self.register("rnd_ipv4", Box::new(RndIntFn));
        self.register("abs_int", Box::new(AbsFn));
        self.register("abs_long", Box::new(AbsFn));
        self.register("abs_double", Box::new(AbsFn));
        self.register("abs_float", Box::new(AbsFn));
        self.register("round_half_even", Box::new(RoundFn));
        self.register("round_down", Box::new(FloorFn));
        self.register("round_up", Box::new(CeilFn));
        self.register("ceil_int", Box::new(CeilFn));
        self.register("ceil_double", Box::new(CeilFn));
        self.register("floor_int", Box::new(FloorFn));
        self.register("floor_double", Box::new(FloorFn));
        self.register("is_null_fn", Box::new(IsNullFn));
        self.register("not_null", Box::new(IsNotNullFn));
        self.register("to_symbol", Box::new(CastStrFn));
        self.register("symbol", Box::new(CastStrFn));
        self.register("typecast", Box::new(TypeOfFn));
        self.register("str_concat", Box::new(ConcatFn));
        self.register("string_concat", Box::new(ConcatFn));
        self.register("pg_catalog_version", Box::new(VersionFn));
        self.register("pg_type_name", Box::new(PgTypeofFn));
        self.register("hash_sha256", Box::new(Sha256Fn));
        self.register("hash_md5", Box::new(Md5Fn));
        self.register("to_pg_date", Box::new(ToDateFn));
        self.register("date_to_str", Box::new(DateFormatFn));
        self.register("date_parse", Box::new(ToTimestampFn));
        self.register("timestamp_parse", Box::new(ToTimestampFn));
        self.register("nanos_to_millis", Box::new(EpochMillisFn));
        self.register("nanos_to_micros", Box::new(EpochMicrosFn));
        self.register("nanos_to_secs", Box::new(EpochSecondsFn));

        // ── Auto-generated cast functions (196+) ─────────────────────
        crate::casts::register_all_casts(self);
        // ── System / catalog / pattern functions (30+) ───────────────
        crate::casts::register_system_functions(self);
        // ── Extra functions (geospatial, array, date/time, string, conditional, table-valued) ──
        crate::functions_extra::register_extra_functions(self);
        // ── QuestDB-compat aliases (per-type aggregates, operators, pg_, JSON, etc.) ──
        crate::functions_compat::register_compat_functions(self);

        // ── Exchange-domain functions (OHLCV, orderbook, tick) ──────
        crate::exchange_functions::register_exchange_functions(self);
    }
}

impl Default for ScalarRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarRegistry {
    /// Return the number of registered functions.
    pub fn len(&self) -> usize {
        self.functions.len()
    }

    /// Return whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

/// Convenience function: evaluate a scalar function by name.
pub fn evaluate_scalar(name: &str, args: &[Value]) -> Result<Value, String> {
    // Internal pseudo-functions for CASE WHEN and IS NULL/IS NOT NULL.
    if name == "__case_when" && args.len() == 3 {
        let cond = matches!(&args[0], Value::I64(v) if *v != 0);
        return Ok(if cond { args[1].clone() } else { args[2].clone() });
    }
    if name == "is_null" && args.len() == 1 {
        return Ok(if args[0] == Value::Null { Value::I64(1) } else { Value::I64(0) });
    }
    if name == "is_not_null" && args.len() == 1 {
        return Ok(if args[0] != Value::Null { Value::I64(1) } else { Value::I64(0) });
    }
    // Use a thread-local registry to avoid re-creating it on every call.
    thread_local! {
        static REGISTRY: ScalarRegistry = ScalarRegistry::new();
    }
    REGISTRY.with(|reg| {
        let func = reg
            .get(name)
            .ok_or_else(|| format!("unknown scalar function: {name}"))?;
        let min = func.min_args();
        let max = func.max_args();
        if args.len() < min || args.len() > max {
            return Err(format!(
                "{name} expects {min}..={max} arguments, got {}",
                args.len()
            ));
        }
        func.evaluate(args)
    })
}

// ---------------------------------------------------------------------------
// Helper: extract an f64 from a Value
// ---------------------------------------------------------------------------

fn to_f64(v: &Value) -> Result<f64, String> {
    match v {
        Value::I64(n) => Ok(*n as f64),
        Value::F64(f) => Ok(*f),
        Value::Timestamp(ns) => Ok(*ns as f64),
        Value::Null => Err("expected numeric value, got NULL".into()),
        Value::Str(s) => s.parse::<f64>().map_err(|e| format!("cannot parse '{s}' as number: {e}")),
    }
}

fn to_i64(v: &Value) -> Result<i64, String> {
    match v {
        Value::I64(n) => Ok(*n),
        Value::F64(f) => Ok(*f as i64),
        Value::Timestamp(ns) => Ok(*ns),
        Value::Null => Err("expected integer value, got NULL".into()),
        Value::Str(s) => s.parse::<i64>().map_err(|e| format!("cannot parse '{s}' as integer: {e}")),
    }
}

fn to_str(v: &Value) -> String {
    match v {
        Value::Str(s) => s.clone(),
        Value::I64(n) => n.to_string(),
        Value::F64(f) => f.to_string(),
        Value::Timestamp(ns) => ns.to_string(),
        Value::Null => String::new(),
    }
}

fn to_timestamp_ns(v: &Value) -> Result<i64, String> {
    match v {
        Value::Timestamp(ns) => Ok(*ns),
        Value::I64(n) => Ok(*n),
        Value::F64(f) => Ok(*f as i64),
        Value::Str(s) => s
            .parse::<i64>()
            .map_err(|e| format!("cannot parse '{s}' as timestamp: {e}")),
        Value::Null => Err("expected timestamp, got NULL".into()),
    }
}

// ===========================================================================
// String functions
// ===========================================================================

struct LengthFn;
impl ScalarFunction for LengthFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::I64(to_str(&args[0]).chars().count() as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct UpperFn;
impl ScalarFunction for UpperFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::Str(to_str(&args[0]).to_uppercase()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct LowerFn;
impl ScalarFunction for LowerFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::Str(to_str(&args[0]).to_lowercase()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct TrimFn;
impl ScalarFunction for TrimFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::Str(to_str(&args[0]).trim().to_string()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct LtrimFn;
impl ScalarFunction for LtrimFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::Str(to_str(&args[0]).trim_start().to_string()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct RtrimFn;
impl ScalarFunction for RtrimFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::Str(to_str(&args[0]).trim_end().to_string()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct SubstringFn;
impl ScalarFunction for SubstringFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let start = to_i64(&args[1])? as usize;
        let len = to_i64(&args[2])? as usize;
        // 1-based start index (SQL convention)
        let start = if start > 0 { start - 1 } else { 0 };
        let result: String = s.chars().skip(start).take(len).collect();
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct ConcatFn;
impl ScalarFunction for ConcatFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        // PostgreSQL semantics: concat() skips NULLs.
        let mut result = String::new();
        for arg in args {
            if !matches!(arg, Value::Null) {
                result.push_str(&to_str(arg));
            }
        }
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { usize::MAX }
}

struct ReplaceFn;
impl ScalarFunction for ReplaceFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let from = to_str(&args[1]);
        let to = to_str(&args[2]);
        if from.is_empty() {
            return Ok(Value::Str(s));
        }
        Ok(Value::Str(s.replace(&from, &to)))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct StartsWithFn;
impl ScalarFunction for StartsWithFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let prefix = to_str(&args[1]);
        Ok(Value::I64(if s.starts_with(&prefix) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct EndsWithFn;
impl ScalarFunction for EndsWithFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let suffix = to_str(&args[1]);
        Ok(Value::I64(if s.ends_with(&suffix) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct ContainsFn;
impl ScalarFunction for ContainsFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let substr = to_str(&args[1]);
        Ok(Value::I64(if s.contains(&substr) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct ReverseFn;
impl ScalarFunction for ReverseFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::Str(to_str(&args[0]).chars().rev().collect()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct RepeatFn;
impl ScalarFunction for RepeatFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let n = to_i64(&args[1])?.max(0) as usize;
        Ok(Value::Str(s.repeat(n)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct LeftFn;
impl ScalarFunction for LeftFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let n = to_i64(&args[1])?.max(0) as usize;
        let result: String = s.chars().take(n).collect();
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct RightFn;
impl ScalarFunction for RightFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let n = to_i64(&args[1])?.max(0) as usize;
        let chars: Vec<char> = s.chars().collect();
        let start = chars.len().saturating_sub(n);
        let result: String = chars[start..].iter().collect();
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

// ===========================================================================
// Math functions
// ===========================================================================

struct AbsFn;
impl ScalarFunction for AbsFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::I64(n) => Ok(Value::I64(n.abs())),
            Value::F64(f) => Ok(Value::F64(f.abs())),
            Value::Null => Ok(Value::Null),
            other => Err(format!("abs: expected numeric, got {other}")),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct RoundFn;
impl ScalarFunction for RoundFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        let decimals = if args.len() > 1 { to_i64(&args[1])? } else { 0 };
        let factor = 10_f64.powi(decimals as i32);
        Ok(Value::F64((x * factor).round() / factor))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

struct FloorFn;
impl ScalarFunction for FloorFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.floor()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CeilFn;
impl ScalarFunction for CeilFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.ceil()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct SqrtFn;
impl ScalarFunction for SqrtFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        if x < 0.0 {
            return Err("sqrt: cannot take square root of negative number".into());
        }
        Ok(Value::F64(x.sqrt()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct PowFn;
impl ScalarFunction for PowFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
            return Ok(Value::Null);
        }
        let x = to_f64(&args[0])?;
        let y = to_f64(&args[1])?;
        Ok(Value::F64(x.powf(y)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct LogFn;
impl ScalarFunction for LogFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        if x <= 0.0 {
            return Err("log: argument must be positive".into());
        }
        Ok(Value::F64(x.ln()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct Log2Fn;
impl ScalarFunction for Log2Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        if x <= 0.0 {
            return Err("log2: argument must be positive".into());
        }
        Ok(Value::F64(x.log2()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct Log10Fn;
impl ScalarFunction for Log10Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        if x <= 0.0 {
            return Err("log10: argument must be positive".into());
        }
        Ok(Value::F64(x.log10()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ExpFn;
impl ScalarFunction for ExpFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.exp()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct SinFn;
impl ScalarFunction for SinFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.sin()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CosFn;
impl ScalarFunction for CosFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.cos()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct TanFn;
impl ScalarFunction for TanFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.tan()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ModFn;
impl ScalarFunction for ModFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
            return Ok(Value::Null);
        }
        match (&args[0], &args[1]) {
            (Value::I64(a), Value::I64(b)) => {
                if *b == 0 {
                    return Err("mod: division by zero".into());
                }
                Ok(Value::I64(a % b))
            }
            _ => {
                let a = to_f64(&args[0])?;
                let b = to_f64(&args[1])?;
                if b == 0.0 {
                    return Err("mod: division by zero".into());
                }
                Ok(Value::F64(a % b))
            }
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct SignFn;
impl ScalarFunction for SignFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        let s = if x > 0.0 {
            1
        } else if x < 0.0 {
            -1
        } else {
            0
        };
        Ok(Value::I64(s))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct PiFn;
impl ScalarFunction for PiFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::F64(std::f64::consts::PI))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct RandomFn;
impl ScalarFunction for RandomFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        // Simple pseudo-random using system time; no external crate needed.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        // Mix bits for a cheap random-ish value in [0, 1).
        let v = ((nanos as u64).wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407)) as f64
            / u64::MAX as f64;
        Ok(Value::F64(v.abs()))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

// ===========================================================================
// Date/Time functions
// ===========================================================================

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MIN: i64 = 60 * NANOS_PER_SEC;
const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MIN;
const NANOS_PER_DAY: i64 = 24 * NANOS_PER_HOUR;

/// Break a nanosecond timestamp into (year, month, day, hour, minute, second).
fn decompose_timestamp(ns: i64) -> (i64, i64, i64, i64, i64, i64) {
    let total_secs = ns.div_euclid(NANOS_PER_SEC);
    let sec_of_day = total_secs.rem_euclid(86400);
    let hour = sec_of_day / 3600;
    let minute = (sec_of_day % 3600) / 60;
    let second = sec_of_day % 60;

    // Days since Unix epoch.
    let mut days = total_secs.div_euclid(86400);

    // Civil date from days (algorithm from Howard Hinnant).
    days += 719468;
    let era = (if days >= 0 { days } else { days - 146096 }) / 146097;
    let doe = (days - era * 146097) as u64; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    (year, m as i64, d as i64, hour, minute, second)
}

/// Compose a civil date (year, month, day) into days since epoch.
fn civil_to_days(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u64;
    let m = month as u64;
    let doy = if m > 2 {
        (153 * (m - 3) + 2) / 5 + day as u64 - 1
    } else {
        (153 * (m + 9) + 2) / 5 + day as u64 - 1
    };
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

/// Return true if `year` is a leap year.
fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Days in a given month (1-based).
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => if is_leap_year(year) { 29 } else { 28 },
        _ => 30,
    }
}

struct NowFn;
impl ScalarFunction for NowFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        let ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        Ok(Value::Timestamp(ns))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct ToTimestampFn;
impl ScalarFunction for ToTimestampFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        Ok(Value::Timestamp(ns))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ExtractYearFn;
impl ScalarFunction for ExtractYearFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, _, _, _, _, _) = decompose_timestamp(ns);
        Ok(Value::I64(year))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ExtractMonthFn;
impl ScalarFunction for ExtractMonthFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (_, month, _, _, _, _) = decompose_timestamp(ns);
        Ok(Value::I64(month))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ExtractDayFn;
impl ScalarFunction for ExtractDayFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (_, _, day, _, _, _) = decompose_timestamp(ns);
        Ok(Value::I64(day))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ExtractHourFn;
impl ScalarFunction for ExtractHourFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (_, _, _, hour, _, _) = decompose_timestamp(ns);
        Ok(Value::I64(hour))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct DateTruncFn;
impl ScalarFunction for DateTruncFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let unit = to_str(&args[0]).to_ascii_lowercase();
        let ns = to_timestamp_ns(&args[1])?;
        let (year, month, day, hour, minute, second) = decompose_timestamp(ns);
        let truncated = match unit.as_str() {
            "second" => {
                let days = civil_to_days(year, month, day);
                days * NANOS_PER_DAY + hour * NANOS_PER_HOUR + minute * NANOS_PER_MIN + second * NANOS_PER_SEC
            }
            "minute" => {
                let days = civil_to_days(year, month, day);
                days * NANOS_PER_DAY + hour * NANOS_PER_HOUR + minute * NANOS_PER_MIN
            }
            "hour" => {
                let days = civil_to_days(year, month, day);
                days * NANOS_PER_DAY + hour * NANOS_PER_HOUR
            }
            "day" => {
                let days = civil_to_days(year, month, day);
                days * NANOS_PER_DAY
            }
            "month" => {
                let days = civil_to_days(year, month, 1);
                days * NANOS_PER_DAY
            }
            "year" => {
                let days = civil_to_days(year, 1, 1);
                days * NANOS_PER_DAY
            }
            other => return Err(format!("date_trunc: unknown unit '{other}'")),
        };
        Ok(Value::Timestamp(truncated))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct DateDiffFn;
impl ScalarFunction for DateDiffFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[1], Value::Null) || matches!(args[2], Value::Null) {
            return Ok(Value::Null);
        }
        let unit = to_str(&args[0]).to_ascii_lowercase();
        let ts1 = to_timestamp_ns(&args[1])?;
        let ts2 = to_timestamp_ns(&args[2])?;
        let diff_ns = ts2 - ts1;
        let result = match unit.as_str() {
            "second" | "seconds" => diff_ns / NANOS_PER_SEC,
            "minute" | "minutes" => diff_ns / NANOS_PER_MIN,
            "hour" | "hours" => diff_ns / NANOS_PER_HOUR,
            "day" | "days" => diff_ns / NANOS_PER_DAY,
            other => return Err(format!("date_diff: unknown unit '{other}'")),
        };
        Ok(Value::I64(result))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct TimestampAddFn;
impl ScalarFunction for TimestampAddFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[2], Value::Null) { return Ok(Value::Null); }
        let unit = to_str(&args[0]).to_ascii_lowercase();
        let amount = to_i64(&args[1])?;
        let ts = to_timestamp_ns(&args[2])?;
        let added = match unit.as_str() {
            "second" | "seconds" => ts + amount * NANOS_PER_SEC,
            "minute" | "minutes" => ts + amount * NANOS_PER_MIN,
            "hour" | "hours" => ts + amount * NANOS_PER_HOUR,
            "day" | "days" => ts + amount * NANOS_PER_DAY,
            "month" | "months" => {
                let (mut year, mut month, day, hour, minute, second) = decompose_timestamp(ts);
                let sub_ns = ts.rem_euclid(NANOS_PER_SEC);
                month += amount;
                // Normalize month to 1..12
                year += (month - 1).div_euclid(12);
                month = (month - 1).rem_euclid(12) + 1;
                let clamped_day = day.min(days_in_month(year, month));
                let days = civil_to_days(year, month, clamped_day);
                days * NANOS_PER_DAY + hour * NANOS_PER_HOUR + minute * NANOS_PER_MIN + second * NANOS_PER_SEC + sub_ns
            }
            "year" | "years" => {
                let (year, month, day, hour, minute, second) = decompose_timestamp(ts);
                let sub_ns = ts.rem_euclid(NANOS_PER_SEC);
                let new_year = year + amount;
                let clamped_day = day.min(days_in_month(new_year, month));
                let days = civil_to_days(new_year, month, clamped_day);
                days * NANOS_PER_DAY + hour * NANOS_PER_HOUR + minute * NANOS_PER_MIN + second * NANOS_PER_SEC + sub_ns
            }
            other => return Err(format!("timestamp_add: unknown unit '{other}'")),
        };
        Ok(Value::Timestamp(added))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct EpochNanosFn;
impl ScalarFunction for EpochNanosFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        Ok(Value::I64(ns))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

// ===========================================================================
// Conditional functions
// ===========================================================================

struct CoalesceFn;
impl ScalarFunction for CoalesceFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args {
            if !matches!(arg, Value::Null) {
                return Ok(arg.clone());
            }
        }
        Ok(Value::Null)
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { usize::MAX }
}

struct NullIfFn;
impl ScalarFunction for NullIfFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args[0] == args[1] {
            Ok(Value::Null)
        } else {
            Ok(args[0].clone())
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct GreatestFn;
impl ScalarFunction for GreatestFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let mut best: Option<&Value> = None;
        for arg in args {
            if matches!(arg, Value::Null) {
                continue;
            }
            best = Some(match best {
                None => arg,
                Some(cur) => if arg > cur { arg } else { cur },
            });
        }
        Ok(best.cloned().unwrap_or(Value::Null))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { usize::MAX }
}

struct LeastFn;
impl ScalarFunction for LeastFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let mut best: Option<&Value> = None;
        for arg in args {
            if matches!(arg, Value::Null) {
                continue;
            }
            best = Some(match best {
                None => arg,
                Some(cur) => if arg < cur { arg } else { cur },
            });
        }
        Ok(best.cloned().unwrap_or(Value::Null))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { usize::MAX }
}

struct IfNullFn;
impl ScalarFunction for IfNullFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) {
            Ok(args[1].clone())
        } else {
            Ok(args[0].clone())
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

// ===========================================================================
// Additional String Functions
// ===========================================================================

struct LpadFn;
impl ScalarFunction for LpadFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let len = to_i64(&args[1])?.max(0) as usize;
        let pad = to_str(&args[2]);
        if pad.is_empty() { return Ok(Value::Str(s)); }
        let char_len = s.chars().count();
        if char_len >= len {
            return Ok(Value::Str(s.chars().take(len).collect()));
        }
        let needed = len - char_len;
        let pad_chars: Vec<char> = pad.chars().collect();
        let mut result = String::with_capacity(len);
        for i in 0..needed {
            result.push(pad_chars[i % pad_chars.len()]);
        }
        result.push_str(&s);
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct RpadFn;
impl ScalarFunction for RpadFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let len = to_i64(&args[1])?.max(0) as usize;
        let pad = to_str(&args[2]);
        if pad.is_empty() { return Ok(Value::Str(s)); }
        let char_len = s.chars().count();
        if char_len >= len {
            return Ok(Value::Str(s.chars().take(len).collect()));
        }
        let needed = len - char_len;
        let pad_chars: Vec<char> = pad.chars().collect();
        let mut result = s;
        for i in 0..needed {
            result.push(pad_chars[i % pad_chars.len()]);
        }
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct SplitPartFn;
impl ScalarFunction for SplitPartFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let delim = to_str(&args[1]);
        let idx = to_i64(&args[2])? as usize;
        let parts: Vec<&str> = s.split(&delim).collect();
        if idx == 0 || idx > parts.len() {
            Ok(Value::Str(String::new()))
        } else {
            Ok(Value::Str(parts[idx - 1].to_string()))
        }
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct RegexpMatchFn;
impl ScalarFunction for RegexpMatchFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let pattern = to_str(&args[1]);
        let re = Regex::new(&pattern).map_err(|e| format!("regexp_match: invalid pattern: {e}"))?;
        Ok(Value::I64(if re.is_match(&s) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct RegexpMatchCiFn;
impl ScalarFunction for RegexpMatchCiFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let pattern = to_str(&args[1]);
        let ci_pattern = format!("(?i){pattern}");
        let re = Regex::new(&ci_pattern).map_err(|e| format!("regexp_match_ci: invalid pattern: {e}"))?;
        Ok(Value::I64(if re.is_match(&s) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct RegexpReplaceFn;
impl ScalarFunction for RegexpReplaceFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let pattern = to_str(&args[1]);
        let replacement = to_str(&args[2]);
        let re = Regex::new(&pattern).map_err(|e| format!("regexp_replace: invalid pattern: {e}"))?;
        Ok(Value::Str(re.replace_all(&s, replacement.as_str()).into_owned()))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct RegexpExtractFn;
impl ScalarFunction for RegexpExtractFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let pattern = to_str(&args[1]);
        let group = if args.len() > 2 { to_i64(&args[2])? as usize } else { 0 };
        let re = Regex::new(&pattern).map_err(|e| format!("regexp_extract: invalid pattern: {e}"))?;
        match re.captures(&s) {
            Some(caps) => {
                match caps.get(group) {
                    Some(m) => Ok(Value::Str(m.as_str().to_string())),
                    None => Ok(Value::Null),
                }
            }
            None => Ok(Value::Null),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 3 }
}

/// Pure-Rust MD5 implementation (RFC 1321).
fn md5_hash(data: &[u8]) -> [u8; 16] {
    // Initial hash values
    let mut a0: u32 = 0x67452301;
    let mut b0: u32 = 0xefcdab89;
    let mut c0: u32 = 0x98badcfe;
    let mut d0: u32 = 0x10325476;

    // Per-round shift amounts
    const S: [u32; 64] = [
        7,12,17,22,7,12,17,22,7,12,17,22,7,12,17,22,
        5,9,14,20,5,9,14,20,5,9,14,20,5,9,14,20,
        4,11,16,23,4,11,16,23,4,11,16,23,4,11,16,23,
        6,10,15,21,6,10,15,21,6,10,15,21,6,10,15,21,
    ];

    // Precomputed T table (floor(2^32 * abs(sin(i+1))))
    const K: [u32; 64] = [
        0xd76aa478,0xe8c7b756,0x242070db,0xc1bdceee,0xf57c0faf,0x4787c62a,0xa8304613,0xfd469501,
        0x698098d8,0x8b44f7af,0xffff5bb1,0x895cd7be,0x6b901122,0xfd987193,0xa679438e,0x49b40821,
        0xf61e2562,0xc040b340,0x265e5a51,0xe9b6c7aa,0xd62f105d,0x02441453,0xd8a1e681,0xe7d3fbc8,
        0x21e1cde6,0xc33707d6,0xf4d50d87,0x455a14ed,0xa9e3e905,0xfcefa3f8,0x676f02d9,0x8d2a4c8a,
        0xfffa3942,0x8771f681,0x6d9d6122,0xfde5380c,0xa4beea44,0x4bdecfa9,0xf6bb4b60,0xbebfbc70,
        0x289b7ec6,0xeaa127fa,0xd4ef3085,0x04881d05,0xd9d4d039,0xe6db99e5,0x1fa27cf8,0xc4ac5665,
        0xf4292244,0x432aff97,0xab9423a7,0xfc93a039,0x655b59c3,0x8f0ccc92,0xffeff47d,0x85845dd1,
        0x6fa87e4f,0xfe2ce6e0,0xa3014314,0x4e0811a1,0xf7537e82,0xbd3af235,0x2ad7d2bb,0xeb86d391,
    ];

    let orig_len_bits = (data.len() as u64) * 8;

    // Padding
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&orig_len_bits.to_le_bytes());

    // Process each 512-bit block
    for chunk in msg.chunks(64) {
        let mut m = [0u32; 16];
        for i in 0..16 {
            m[i] = u32::from_le_bytes([chunk[4*i], chunk[4*i+1], chunk[4*i+2], chunk[4*i+3]]);
        }

        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);

        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | ((!b) & d), i),
                16..=31 => ((d & b) | ((!d) & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | (!d)), (7 * i) % 16),
            };

            let f = f.wrapping_add(a).wrapping_add(K[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f.rotate_left(S[i]));
        }

        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut result = [0u8; 16];
    result[0..4].copy_from_slice(&a0.to_le_bytes());
    result[4..8].copy_from_slice(&b0.to_le_bytes());
    result[8..12].copy_from_slice(&c0.to_le_bytes());
    result[12..16].copy_from_slice(&d0.to_le_bytes());
    result
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

struct Md5Fn;
impl ScalarFunction for Md5Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let hash = md5_hash(s.as_bytes());
        Ok(Value::Str(bytes_to_hex(&hash)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Pure-Rust SHA-256 implementation (FIPS 180-4).
fn sha256_hash(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
        0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
        0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
        0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
        0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
        0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
        0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
        0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2,
    ];

    let mut h: [u32; 8] = [
        0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,
        0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19,
    ];

    let orig_len_bits = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&orig_len_bits.to_be_bytes());

    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([chunk[4*i], chunk[4*i+1], chunk[4*i+2], chunk[4*i+3]]);
        }
        for i in 16..64 {
            let s0 = w[i-15].rotate_right(7) ^ w[i-15].rotate_right(18) ^ (w[i-15] >> 3);
            let s1 = w[i-2].rotate_right(17) ^ w[i-2].rotate_right(19) ^ (w[i-2] >> 10);
            w[i] = w[i-16].wrapping_add(s0).wrapping_add(w[i-7]).wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut result = [0u8; 32];
    for i in 0..8 {
        result[4*i..4*i+4].copy_from_slice(&h[i].to_be_bytes());
    }
    result
}

struct Sha256Fn;
impl ScalarFunction for Sha256Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let hash = sha256_hash(s.as_bytes());
        Ok(Value::Str(bytes_to_hex(&hash)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ToCharFn;
impl ScalarFunction for ToCharFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        // Simple formatting: for timestamps, use decompose; for numbers, use format string
        match &args[0] {
            Value::Timestamp(ns) => {
                let (year, month, day, hour, minute, second) = decompose_timestamp(*ns);
                let fmt = if args.len() > 1 { to_str(&args[1]) } else { "YYYY-MM-DD HH24:MI:SS".to_string() };
                let result = fmt
                    .replace("YYYY", &format!("{year:04}"))
                    .replace("MM", &format!("{month:02}"))
                    .replace("DD", &format!("{day:02}"))
                    .replace("HH24", &format!("{hour:02}"))
                    .replace("HH", &format!("{hour:02}"))
                    .replace("MI", &format!("{minute:02}"))
                    .replace("SS", &format!("{second:02}"));
                Ok(Value::Str(result))
            }
            other => Ok(Value::Str(to_str(other))),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

struct CharLengthFn;
impl ScalarFunction for CharLengthFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::I64(to_str(&args[0]).chars().count() as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct PositionFn;
impl ScalarFunction for PositionFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let substr = to_str(&args[0]);
        let s = to_str(&args[1]);
        match s.find(&substr) {
            Some(pos) => {
                // Convert byte offset to 1-based char position
                let char_pos = s[..pos].chars().count() + 1;
                Ok(Value::I64(char_pos as i64))
            }
            None => Ok(Value::I64(0)),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct OverlayFn;
impl ScalarFunction for OverlayFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let replacement = to_str(&args[1]);
        let start = (to_i64(&args[2])? - 1).max(0) as usize; // 1-based to 0-based
        let len = to_i64(&args[3])?.max(0) as usize;
        let chars: Vec<char> = s.chars().collect();
        let mut result = String::new();
        for &ch in chars.iter().take(start) {
            result.push(ch);
        }
        result.push_str(&replacement);
        for &ch in chars.iter().skip(start + len) {
            result.push(ch);
        }
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 4 }
    fn max_args(&self) -> usize { 4 }
}

struct TranslateFn;
impl ScalarFunction for TranslateFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let from_chars: Vec<char> = to_str(&args[1]).chars().collect();
        let to_chars: Vec<char> = to_str(&args[2]).chars().collect();
        let result: String = s.chars().filter_map(|c| {
            match from_chars.iter().position(|&fc| fc == c) {
                Some(i) => {
                    if i < to_chars.len() {
                        Some(to_chars[i])
                    } else {
                        None // character removed
                    }
                }
                None => Some(c),
            }
        }).collect();
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct InitcapFn;
impl ScalarFunction for InitcapFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let mut result = String::with_capacity(s.len());
        let mut capitalize_next = true;
        for c in s.chars() {
            if c.is_alphanumeric() {
                if capitalize_next {
                    result.extend(c.to_uppercase());
                    capitalize_next = false;
                } else {
                    result.extend(c.to_lowercase());
                }
            } else {
                result.push(c);
                capitalize_next = true;
            }
        }
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

// Base64 encoding/decoding (pure Rust, no external crate)
const BASE64_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(BASE64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(BASE64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(BASE64_CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(BASE64_CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

fn base64_decode(encoded: &str) -> Result<Vec<u8>, String> {
    fn b64_val(c: u8) -> Result<u8, String> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            b'=' => Ok(0),
            _ => Err(format!("invalid base64 character: {c}")),
        }
    }
    let bytes: Vec<u8> = encoded.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if bytes.len() % 4 != 0 {
        return Err("invalid base64 length".into());
    }
    let mut result = Vec::new();
    for chunk in bytes.chunks(4) {
        let a = b64_val(chunk[0])? as u32;
        let b = b64_val(chunk[1])? as u32;
        let c = b64_val(chunk[2])? as u32;
        let d = b64_val(chunk[3])? as u32;
        let triple = (a << 18) | (b << 12) | (c << 6) | d;
        result.push(((triple >> 16) & 0xFF) as u8);
        if chunk[2] != b'=' {
            result.push(((triple >> 8) & 0xFF) as u8);
        }
        if chunk[3] != b'=' {
            result.push((triple & 0xFF) as u8);
        }
    }
    Ok(result)
}

struct EncodeFn;
impl ScalarFunction for EncodeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let data = to_str(&args[0]);
        let format = to_str(&args[1]).to_ascii_lowercase();
        match format.as_str() {
            "base64" => Ok(Value::Str(base64_encode(data.as_bytes()))),
            other => Err(format!("encode: unsupported format '{other}', use 'base64'")),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct DecodeFn;
impl ScalarFunction for DecodeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let data = to_str(&args[0]);
        let format = to_str(&args[1]).to_ascii_lowercase();
        match format.as_str() {
            "base64" => {
                let bytes = base64_decode(&data)?;
                Ok(Value::Str(String::from_utf8(bytes).map_err(|e| format!("decode: invalid UTF-8: {e}"))?))
            }
            other => Err(format!("decode: unsupported format '{other}', use 'base64'")),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct QuoteIdentFn;
impl ScalarFunction for QuoteIdentFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        Ok(Value::Str(format!("\"{}\"", s.replace('"', "\"\""))))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct QuoteLiteralFn;
impl ScalarFunction for QuoteLiteralFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        Ok(Value::Str(format!("'{}'", s.replace('\'', "''"))))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct FormatFn;
impl ScalarFunction for FormatFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.is_empty() { return Err("format: requires at least 1 argument".into()); }
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let template = to_str(&args[0]);
        let mut result = template;
        // Replace %s placeholders with arguments in order
        for arg in &args[1..] {
            if let Some(pos) = result.find("%s") {
                let replacement = to_str(arg);
                result = format!("{}{}{}", &result[..pos], replacement, &result[pos + 2..]);
            }
        }
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { usize::MAX }
}

struct AsciiFn;
impl ScalarFunction for AsciiFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        match s.chars().next() {
            Some(c) => Ok(Value::I64(c as i64)),
            None => Ok(Value::I64(0)),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ChrFn;
impl ScalarFunction for ChrFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let code = to_i64(&args[0])? as u32;
        match char::from_u32(code) {
            Some(c) => Ok(Value::Str(c.to_string())),
            None => Err(format!("chr: invalid character code {code}")),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

// ===========================================================================
// Type Casting Functions
// ===========================================================================

struct CastIntFn;
impl ScalarFunction for CastIntFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::I64(to_i64(&args[0])?))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CastFloatFn;
impl ScalarFunction for CastFloatFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CastStrFn;
impl ScalarFunction for CastStrFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::Str(to_str(&args[0])))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CastBoolFn;
impl ScalarFunction for CastBoolFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::I64(n) => Ok(Value::I64(if *n != 0 { 1 } else { 0 })),
            Value::F64(f) => Ok(Value::I64(if *f != 0.0 { 1 } else { 0 })),
            Value::Str(s) => {
                let lower = s.to_ascii_lowercase();
                Ok(Value::I64(if lower == "true" || lower == "1" || lower == "yes" { 1 } else { 0 }))
            }
            Value::Timestamp(_) => Ok(Value::I64(1)),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CastTimestampFn;
impl ScalarFunction for CastTimestampFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        Ok(Value::Timestamp(ns))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ToDateFn;
impl ScalarFunction for ToDateFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        // Single-arg form: convert integer/timestamp to date (truncate to day boundary)
        if args.len() == 1 {
            let ns = to_timestamp_ns(&args[0])?;
            let day_ns = (ns / NANOS_PER_DAY) * NANOS_PER_DAY;
            return Ok(Value::Timestamp(day_ns));
        }
        let s = to_str(&args[0]);
        let fmt = to_str(&args[1]).to_ascii_lowercase();
        // Support basic YYYY-MM-DD format
        match fmt.as_str() {
            "yyyy-mm-dd" => {
                let parts: Vec<&str> = s.split('-').collect();
                if parts.len() != 3 {
                    return Err(format!("to_date: cannot parse '{s}' with format '{fmt}'"));
                }
                let year: i64 = parts[0].parse().map_err(|_| format!("to_date: invalid year"))?;
                let month: i64 = parts[1].parse().map_err(|_| format!("to_date: invalid month"))?;
                let day: i64 = parts[2].parse().map_err(|_| format!("to_date: invalid day"))?;
                let days = civil_to_days(year, month, day);
                Ok(Value::Timestamp(days * NANOS_PER_DAY))
            }
            _ => Err(format!("to_date: unsupported format '{fmt}', use 'yyyy-mm-dd'")),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

struct ToNumberFn;
impl ScalarFunction for ToNumberFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::I64(_) | Value::F64(_) => Ok(args[0].clone()),
            Value::Str(s) => {
                if let Ok(n) = s.parse::<i64>() {
                    Ok(Value::I64(n))
                } else if let Ok(f) = s.parse::<f64>() {
                    Ok(Value::F64(f))
                } else {
                    Err(format!("to_number: cannot parse '{s}'"))
                }
            }
            Value::Timestamp(ns) => Ok(Value::I64(*ns)),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct TypeOfFn;
impl ScalarFunction for TypeOfFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let type_name = match &args[0] {
            Value::Null => "null",
            Value::I64(_) => "i64",
            Value::F64(_) => "f64",
            Value::Str(_) => "string",
            Value::Timestamp(_) => "timestamp",
        };
        Ok(Value::Str(type_name.to_string()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsNullFn;
impl ScalarFunction for IsNullFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(if matches!(args[0], Value::Null) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsNotNullFn;
impl ScalarFunction for IsNotNullFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(if matches!(args[0], Value::Null) { 0 } else { 1 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct NullIfZeroFn;
impl ScalarFunction for NullIfZeroFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::I64(0) => Ok(Value::Null),
            Value::F64(f) if *f == 0.0 => Ok(Value::Null),
            Value::Null => Ok(Value::Null),
            other => Ok(other.clone()),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

// ===========================================================================
// Additional Math Functions
// ===========================================================================

struct DegreesFn;
impl ScalarFunction for DegreesFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.to_degrees()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct RadiansFn;
impl ScalarFunction for RadiansFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.to_radians()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct Atan2Fn;
impl ScalarFunction for Atan2Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let y = to_f64(&args[0])?;
        let x = to_f64(&args[1])?;
        Ok(Value::F64(y.atan2(x)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct AsinFn;
impl ScalarFunction for AsinFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        if x < -1.0 || x > 1.0 {
            return Err("asin: argument must be in [-1, 1]".into());
        }
        Ok(Value::F64(x.asin()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct AcosFn;
impl ScalarFunction for AcosFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        if x < -1.0 || x > 1.0 {
            return Err("acos: argument must be in [-1, 1]".into());
        }
        Ok(Value::F64(x.acos()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct AtanFn;
impl ScalarFunction for AtanFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.atan()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct SinhFn;
impl ScalarFunction for SinhFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.sinh()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CoshFn;
impl ScalarFunction for CoshFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.cosh()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct TanhFn;
impl ScalarFunction for TanhFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.tanh()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CbrtFn;
impl ScalarFunction for CbrtFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::F64(to_f64(&args[0])?.cbrt()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct FactorialFn;
impl ScalarFunction for FactorialFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let n = to_i64(&args[0])?;
        if n < 0 {
            return Err("factorial: argument must be non-negative".into());
        }
        if n > 20 {
            return Err("factorial: argument too large (max 20)".into());
        }
        let mut result: i64 = 1;
        for i in 2..=n {
            result = result.checked_mul(i).ok_or("factorial: overflow")?;
        }
        Ok(Value::I64(result))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

fn compute_gcd(mut a: i64, mut b: i64) -> i64 {
    a = a.abs();
    b = b.abs();
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

struct GcdFn;
impl ScalarFunction for GcdFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let a = to_i64(&args[0])?;
        let b = to_i64(&args[1])?;
        Ok(Value::I64(compute_gcd(a, b)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct LcmFn;
impl ScalarFunction for LcmFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let a = to_i64(&args[0])?;
        let b = to_i64(&args[1])?;
        if a == 0 && b == 0 {
            return Ok(Value::I64(0));
        }
        let g = compute_gcd(a, b);
        Ok(Value::I64((a / g * b).abs()))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct BitAndFn;
impl ScalarFunction for BitAndFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let a = to_i64(&args[0])?;
        let b = to_i64(&args[1])?;
        Ok(Value::I64(a & b))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct BitOrFn;
impl ScalarFunction for BitOrFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let a = to_i64(&args[0])?;
        let b = to_i64(&args[1])?;
        Ok(Value::I64(a | b))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct BitXorFn;
impl ScalarFunction for BitXorFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let a = to_i64(&args[0])?;
        let b = to_i64(&args[1])?;
        Ok(Value::I64(a ^ b))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct BitNotFn;
impl ScalarFunction for BitNotFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let a = to_i64(&args[0])?;
        Ok(Value::I64(!a))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct BitShiftLeftFn;
impl ScalarFunction for BitShiftLeftFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let a = to_i64(&args[0])?;
        let n = to_i64(&args[1])? as u32;
        Ok(Value::I64(a.wrapping_shl(n)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct BitShiftRightFn;
impl ScalarFunction for BitShiftRightFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let a = to_i64(&args[0])?;
        let n = to_i64(&args[1])? as u32;
        Ok(Value::I64(a.wrapping_shr(n)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct TruncFn;
impl ScalarFunction for TruncFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        let d = if args.len() > 1 { to_i64(&args[1])? } else { 0 };
        let factor = 10_f64.powi(d as i32);
        Ok(Value::F64((x * factor).trunc() / factor))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

struct DivFn;
impl ScalarFunction for DivFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let a = to_i64(&args[0])?;
        let b = to_i64(&args[1])?;
        if b == 0 {
            return Err("div: division by zero".into());
        }
        Ok(Value::I64(a / b))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct WidthBucketFn;
impl ScalarFunction for WidthBucketFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let value = to_f64(&args[0])?;
        let min = to_f64(&args[1])?;
        let max = to_f64(&args[2])?;
        let buckets = to_i64(&args[3])?;
        if buckets <= 0 {
            return Err("width_bucket: buckets must be positive".into());
        }
        if min >= max {
            return Err("width_bucket: min must be less than max".into());
        }
        if value < min {
            Ok(Value::I64(0))
        } else if value >= max {
            Ok(Value::I64(buckets + 1))
        } else {
            let bucket = ((value - min) / (max - min) * buckets as f64).floor() as i64 + 1;
            Ok(Value::I64(bucket))
        }
    }
    fn min_args(&self) -> usize { 4 }
    fn max_args(&self) -> usize { 4 }
}

// ===========================================================================
// Additional Date/Time Functions
// ===========================================================================

struct ExtractMinuteFn;
impl ScalarFunction for ExtractMinuteFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (_, _, _, _, minute, _) = decompose_timestamp(ns);
        Ok(Value::I64(minute))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ExtractSecondFn;
impl ScalarFunction for ExtractSecondFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (_, _, _, _, _, second) = decompose_timestamp(ns);
        Ok(Value::I64(second))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ExtractWeekFn;
impl ScalarFunction for ExtractWeekFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let total_secs = ns.div_euclid(NANOS_PER_SEC);
        let days = total_secs.div_euclid(86400);
        // ISO week: Jan 1 1970 was Thursday (day 4, where Monday=1)
        // day_of_week: 1=Monday, 7=Sunday
        let (year, month, day, _, _, _) = decompose_timestamp(ns);
        let doy = day_of_year(year, month, day);
        // Day of week for Jan 1 of this year
        let jan1_days = civil_to_days(year, 1, 1);
        let jan1_dow = ((jan1_days + 3) % 7 + 7) % 7 + 1; // 1=Monday, 7=Sunday
        // ISO week number
        let _ = days; // suppress warning
        let week = (doy + jan1_dow as i64 - 2) / 7 + 1;
        Ok(Value::I64(week.max(1).min(53)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Day of year (1-based)
fn day_of_year(year: i64, month: i64, day: i64) -> i64 {
    let jan1 = civil_to_days(year, 1, 1);
    let this_day = civil_to_days(year, month, day);
    this_day - jan1 + 1
}

struct ExtractDayOfWeekFn;
impl ScalarFunction for ExtractDayOfWeekFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let total_secs = ns.div_euclid(NANOS_PER_SEC);
        let days = total_secs.div_euclid(86400);
        // Jan 1 1970 was Thursday. 0=Sunday convention:
        // Thursday = 4
        let dow = ((days + 4) % 7 + 7) % 7;
        Ok(Value::I64(dow))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ExtractDayOfYearFn;
impl ScalarFunction for ExtractDayOfYearFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, month, day, _, _, _) = decompose_timestamp(ns);
        Ok(Value::I64(day_of_year(year, month, day)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ExtractQuarterFn;
impl ScalarFunction for ExtractQuarterFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (_, month, _, _, _, _) = decompose_timestamp(ns);
        Ok(Value::I64((month - 1) / 3 + 1))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ToStrTimestampFn;
impl ScalarFunction for ToStrTimestampFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, month, day, hour, minute, second) = decompose_timestamp(ns);
        let fmt = if args.len() > 1 { to_str(&args[1]) } else { "YYYY-MM-DD HH24:MI:SS".to_string() };
        let result = fmt
            .replace("YYYY", &format!("{year:04}"))
            .replace("MM", &format!("{month:02}"))
            .replace("DD", &format!("{day:02}"))
            .replace("HH24", &format!("{hour:02}"))
            .replace("HH", &format!("{hour:02}"))
            .replace("MI", &format!("{minute:02}"))
            .replace("SS", &format!("{second:02}"));
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

struct DatePartFn;
impl ScalarFunction for DatePartFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let unit = to_str(&args[0]).to_ascii_lowercase();
        let ns = to_timestamp_ns(&args[1])?;
        let (year, month, day, hour, minute, second) = decompose_timestamp(ns);
        match unit.as_str() {
            "year" => Ok(Value::I64(year)),
            "month" => Ok(Value::I64(month)),
            "day" => Ok(Value::I64(day)),
            "hour" => Ok(Value::I64(hour)),
            "minute" => Ok(Value::I64(minute)),
            "second" => Ok(Value::I64(second)),
            "quarter" => Ok(Value::I64((month - 1) / 3 + 1)),
            "day_of_year" | "doy" => Ok(Value::I64(day_of_year(year, month, day))),
            other => Err(format!("date_part: unknown unit '{other}'")),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct AgeFn;
impl ScalarFunction for AgeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let ts1 = to_timestamp_ns(&args[0])?;
        let ts2 = to_timestamp_ns(&args[1])?;
        let diff_ns = ts2 - ts1;
        Ok(Value::I64(diff_ns))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct MakeTimestampFn;
impl ScalarFunction for MakeTimestampFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args { if matches!(arg, Value::Null) { return Ok(Value::Null); } }
        let year = to_i64(&args[0])?;
        let month = to_i64(&args[1])?;
        let day = to_i64(&args[2])?;
        let hour = to_i64(&args[3])?;
        let minute = to_i64(&args[4])?;
        let second = to_i64(&args[5])?;
        let days = civil_to_days(year, month, day);
        let ns = days * NANOS_PER_DAY + hour * NANOS_PER_HOUR + minute * NANOS_PER_MIN + second * NANOS_PER_SEC;
        Ok(Value::Timestamp(ns))
    }
    fn min_args(&self) -> usize { 6 }
    fn max_args(&self) -> usize { 6 }
}

struct TimestampCeilFn;
impl ScalarFunction for TimestampCeilFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let unit = to_str(&args[0]).to_ascii_lowercase();
        let ns = to_timestamp_ns(&args[1])?;
        let (year, month, day, hour, minute, second) = decompose_timestamp(ns);
        let truncated = match unit.as_str() {
            "second" => {
                let days = civil_to_days(year, month, day);
                days * NANOS_PER_DAY + hour * NANOS_PER_HOUR + minute * NANOS_PER_MIN + second * NANOS_PER_SEC
            }
            "minute" => {
                let days = civil_to_days(year, month, day);
                days * NANOS_PER_DAY + hour * NANOS_PER_HOUR + minute * NANOS_PER_MIN
            }
            "hour" => {
                let days = civil_to_days(year, month, day);
                days * NANOS_PER_DAY + hour * NANOS_PER_HOUR
            }
            "day" => {
                let days = civil_to_days(year, month, day);
                days * NANOS_PER_DAY
            }
            "month" => {
                let days = civil_to_days(year, month, 1);
                days * NANOS_PER_DAY
            }
            "year" => {
                let days = civil_to_days(year, 1, 1);
                days * NANOS_PER_DAY
            }
            other => return Err(format!("timestamp_ceil: unknown unit '{other}'")),
        };
        if truncated == ns {
            Ok(Value::Timestamp(ns))
        } else {
            // Round up to next unit boundary
            let next = match unit.as_str() {
                "second" => truncated + NANOS_PER_SEC,
                "minute" => truncated + NANOS_PER_MIN,
                "hour" => truncated + NANOS_PER_HOUR,
                "day" => truncated + NANOS_PER_DAY,
                "month" => {
                    let (ny, nm) = if month == 12 { (year + 1, 1) } else { (year, month + 1) };
                    civil_to_days(ny, nm, 1) * NANOS_PER_DAY
                }
                "year" => civil_to_days(year + 1, 1, 1) * NANOS_PER_DAY,
                _ => unreachable!(),
            };
            Ok(Value::Timestamp(next))
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct IntervalToNanosFn;
impl ScalarFunction for IntervalToNanosFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]).trim().to_string();
        // Parse strings like "1h", "30m", "2d", "500ms", "1s"
        let (num_str, unit) = if s.ends_with("ms") {
            (&s[..s.len()-2], "ms")
        } else if s.ends_with("ns") {
            (&s[..s.len()-2], "ns")
        } else if s.ends_with("us") {
            (&s[..s.len()-2], "us")
        } else {
            let split_pos = s.len() - 1;
            (&s[..split_pos], &s[split_pos..])
        };
        let num: i64 = num_str.trim().parse().map_err(|e| format!("interval_to_nanos: cannot parse number: {e}"))?;
        let nanos = match unit {
            "ns" => num,
            "us" => num * 1_000,
            "ms" => num * 1_000_000,
            "s" => num * NANOS_PER_SEC,
            "m" => num * NANOS_PER_MIN,
            "h" => num * NANOS_PER_HOUR,
            "d" => num * NANOS_PER_DAY,
            other => return Err(format!("interval_to_nanos: unknown unit '{other}'")),
        };
        Ok(Value::I64(nanos))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct DaysInMonthFn;
impl ScalarFunction for DaysInMonthFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, month, _, _, _, _) = decompose_timestamp(ns);
        Ok(Value::I64(days_in_month(year, month)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsLeapYearFn;
impl ScalarFunction for IsLeapYearFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, _, _, _, _, _) = decompose_timestamp(ns);
        Ok(Value::I64(if is_leap_year(year) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct FirstOfMonthFn;
impl ScalarFunction for FirstOfMonthFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, month, _, _, _, _) = decompose_timestamp(ns);
        let days = civil_to_days(year, month, 1);
        Ok(Value::Timestamp(days * NANOS_PER_DAY))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct LastOfMonthFn;
impl ScalarFunction for LastOfMonthFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, month, _, _, _, _) = decompose_timestamp(ns);
        let last_day = days_in_month(year, month);
        let days = civil_to_days(year, month, last_day);
        Ok(Value::Timestamp(days * NANOS_PER_DAY))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct NextDayFn;
impl ScalarFunction for NextDayFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let target_name = to_str(&args[1]).to_ascii_lowercase();
        let target_dow = match target_name.as_str() {
            "sunday" | "sun" => 0_i64,
            "monday" | "mon" => 1,
            "tuesday" | "tue" => 2,
            "wednesday" | "wed" => 3,
            "thursday" | "thu" => 4,
            "friday" | "fri" => 5,
            "saturday" | "sat" => 6,
            other => return Err(format!("next_day: unknown day name '{other}'")),
        };
        let total_secs = ns.div_euclid(NANOS_PER_SEC);
        let days = total_secs.div_euclid(86400);
        // Current day of week (0=Sunday)
        let current_dow = ((days + 4) % 7 + 7) % 7;
        let mut diff = target_dow - current_dow;
        if diff <= 0 { diff += 7; }
        Ok(Value::Timestamp((days + diff) * NANOS_PER_DAY))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct MonthsBetweenFn;
impl ScalarFunction for MonthsBetweenFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let ts1 = to_timestamp_ns(&args[0])?;
        let ts2 = to_timestamp_ns(&args[1])?;
        let (y1, m1, _, _, _, _) = decompose_timestamp(ts1);
        let (y2, m2, _, _, _, _) = decompose_timestamp(ts2);
        let months = (y1 - y2) * 12 + (m1 - m2);
        Ok(Value::I64(months))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct YearsBetweenFn;
impl ScalarFunction for YearsBetweenFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let ts1 = to_timestamp_ns(&args[0])?;
        let ts2 = to_timestamp_ns(&args[1])?;
        let (y1, _, _, _, _, _) = decompose_timestamp(ts1);
        let (y2, _, _, _, _, _) = decompose_timestamp(ts2);
        Ok(Value::I64(y1 - y2))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

// ===========================================================================
// Cast functions
// ===========================================================================

struct CastToIntFn;
impl ScalarFunction for CastToIntFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::I64(n) => Ok(Value::I64(*n)),
            Value::F64(f) => Ok(Value::I64(*f as i64)),
            Value::Str(s) => s.parse::<i64>()
                .map(Value::I64)
                .or_else(|_| s.parse::<f64>().map(|f| Value::I64(f as i64)))
                .map_err(|_| format!("cannot cast '{}' to integer", s)),
            Value::Timestamp(ns) => Ok(Value::I64(*ns)),
            Value::Null => Ok(Value::Null),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CastToFloatFn;
impl ScalarFunction for CastToFloatFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::F64(f) => Ok(Value::F64(*f)),
            Value::I64(n) => Ok(Value::F64(*n as f64)),
            Value::Str(s) => s.parse::<f64>()
                .map(Value::F64)
                .map_err(|_| format!("cannot cast '{}' to float", s)),
            Value::Timestamp(ns) => Ok(Value::F64(*ns as f64)),
            Value::Null => Ok(Value::Null),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CastToStrFn;
impl ScalarFunction for CastToStrFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::Str(to_str(&args[0])))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CastToTimestampFn;
impl ScalarFunction for CastToTimestampFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::Timestamp(ns) => Ok(Value::Timestamp(*ns)),
            Value::I64(n) => Ok(Value::Timestamp(*n)),
            Value::F64(f) => Ok(Value::Timestamp(*f as i64)),
            Value::Str(s) => {
                // Try parsing as integer nanoseconds first.
                if let Ok(ns) = s.parse::<i64>() {
                    return Ok(Value::Timestamp(ns));
                }
                // Try parsing ISO 8601 date format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
                if let Some(ts) = parse_iso_timestamp(s) {
                    return Ok(Value::Timestamp(ts));
                }
                Err(format!("cannot cast '{}' to timestamp", s))
            }
            Value::Null => Ok(Value::Null),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Parse a simple ISO 8601 date/datetime string to nanoseconds since Unix epoch.
fn parse_iso_timestamp(s: &str) -> Option<i64> {
    // Parse YYYY-MM-DD
    let parts: Vec<&str> = s.split('T').collect();
    let date_part = parts.first()?;
    let date_parts: Vec<&str> = date_part.split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }
    let year: i64 = date_parts[0].parse().ok()?;
    let month: i64 = date_parts[1].parse().ok()?;
    let day: i64 = date_parts[2].parse().ok()?;

    // Simple days-from-epoch calculation (not fully precise for all edge cases).
    let mut total_days: i64 = 0;
    for y in 1970..year {
        total_days += if is_leap(y) { 366 } else { 365 };
    }
    let days_in_months = [31, if is_leap(year) { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 0..(month - 1) as usize {
        total_days += days_in_months.get(m).copied().unwrap_or(30) as i64;
    }
    total_days += day - 1;

    let mut secs = total_days * 86400;

    // Parse optional time part HH:MM:SS
    if let Some(time_part) = parts.get(1) {
        let time_parts: Vec<&str> = time_part.split(':').collect();
        if let Some(h) = time_parts.first().and_then(|s| s.parse::<i64>().ok()) {
            secs += h * 3600;
        }
        if let Some(m) = time_parts.get(1).and_then(|s| s.parse::<i64>().ok()) {
            secs += m * 60;
        }
        if let Some(s_str) = time_parts.get(2) {
            // Handle fractional seconds
            if let Ok(s_val) = s_str.parse::<i64>() {
                secs += s_val;
            }
        }
    }

    Some(secs * 1_000_000_000)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

// ===========================================================================
// Random / testing functions (rnd_*)
// ===========================================================================

/// Simple pseudo-random number generator (xorshift64) seeded from system time.
/// Not cryptographically secure, but good enough for testing data generation.
fn quick_random_u64() -> u64 {
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    // xorshift64
    // Mix in a counter to avoid same-nanosecond collisions.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut x = seed.wrapping_add(COUNTER.fetch_add(1, Ordering::Relaxed));
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// rnd_int(lo, hi) -> random i64 in [lo, hi]
struct RndIntFn;
impl ScalarFunction for RndIntFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let lo = match args.first() { Some(Value::I64(n)) => *n, Some(Value::F64(n)) => *n as i64, _ => 0 };
        let hi = match args.get(1) { Some(Value::I64(n)) => *n, Some(Value::F64(n)) => *n as i64, _ => i64::MAX };
        if hi <= lo { return Ok(Value::I64(lo)); }
        let range = (hi as u64).wrapping_sub(lo as u64).wrapping_add(1);
        let r = if range == 0 { quick_random_u64() } else { quick_random_u64() % range };
        Ok(Value::I64(lo.wrapping_add(r as i64)))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 2 }
}

/// rnd_double() -> random f64 in [0.0, 1.0)
struct RndDoubleFn;
impl ScalarFunction for RndDoubleFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        let r = quick_random_u64() as f64 / u64::MAX as f64;
        Ok(Value::F64(r.abs()))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

/// rnd_float() -> random f32 as f64
struct RndFloatFn;
impl ScalarFunction for RndFloatFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        let r = (quick_random_u64() as f32 / u32::MAX as f32).abs();
        Ok(Value::F64(r as f64))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

/// rnd_str('BTC','ETH','SOL') -> random pick from arguments
struct RndStrFn;
impl ScalarFunction for RndStrFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.is_empty() {
            // Generate a random short string
            let len = (quick_random_u64() % 8 + 1) as usize;
            let s: String = (0..len).map(|_| (b'a' + (quick_random_u64() % 26) as u8) as char).collect();
            return Ok(Value::Str(s));
        }
        let idx = quick_random_u64() as usize % args.len();
        match &args[idx] {
            Value::Str(s) => Ok(Value::Str(s.clone())),
            other => Ok(Value::Str(format!("{other}"))),
        }
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { usize::MAX }
}

/// rnd_boolean() -> 0 or 1
struct RndBooleanFn;
impl ScalarFunction for RndBooleanFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64((quick_random_u64() & 1) as i64))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

/// rnd_timestamp('2024-01-01','2024-12-31') -> random timestamp in range
struct RndTimestampFn;
impl ScalarFunction for RndTimestampFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let lo = match args.first() {
            Some(Value::Timestamp(ns)) => *ns,
            Some(Value::I64(ns)) => *ns,
            Some(Value::Str(s)) => parse_timestamp_str(s).map_err(|e| format!("rnd_timestamp: {e}"))?,
            _ => 0,
        };
        let hi = match args.get(1) {
            Some(Value::Timestamp(ns)) => *ns,
            Some(Value::I64(ns)) => *ns,
            Some(Value::Str(s)) => parse_timestamp_str(s).map_err(|e| format!("rnd_timestamp: {e}"))?,
            _ => lo + 86400_000_000_000, // default: +1 day
        };
        if hi <= lo { return Ok(Value::Timestamp(lo)); }
        let range = (hi - lo) as u64;
        let r = quick_random_u64() % range;
        Ok(Value::Timestamp(lo + r as i64))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 2 }
}

/// Parse a date string like "2024-01-01" into nanoseconds since epoch.
fn parse_timestamp_str(s: &str) -> Result<i64, String> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(format!("invalid date format: '{s}', expected YYYY-MM-DD"));
    }
    let year: i64 = parts[0].parse().map_err(|_| format!("invalid year: {}", parts[0]))?;
    let month: i64 = parts[1].parse().map_err(|_| format!("invalid month: {}", parts[1]))?;
    let day: i64 = parts[2].parse().map_err(|_| format!("invalid day: {}", parts[2]))?;
    // Simple days-from-epoch calculation.
    let days = civil_to_days(year, month, day);
    Ok(days * 86400_000_000_000)
}

/// rnd_uuid4() -> random UUID v4 string
struct RndUuid4Fn;
impl ScalarFunction for RndUuid4Fn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        let hi = quick_random_u64();
        let lo = quick_random_u64();
        // UUID v4: set version bits (4) and variant bits (10xx).
        let hi = (hi & 0xFFFF_FFFF_FFFF_0FFF) | 0x0000_0000_0000_4000;
        let lo = (lo & 0x3FFF_FFFF_FFFF_FFFF) | 0x8000_0000_0000_0000;
        let uuid = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            (hi >> 32) as u32,
            ((hi >> 16) & 0xFFFF) as u16,
            (hi & 0xFFFF) as u16,
            ((lo >> 48) & 0xFFFF) as u16,
            lo & 0x0000_FFFF_FFFF_FFFF,
        );
        Ok(Value::Str(uuid))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

// ===========================================================================
// New date/time functions
// ===========================================================================

const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1_000;

struct EpochSecondsFn;
impl ScalarFunction for EpochSecondsFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        Ok(Value::I64(ns / NANOS_PER_SEC))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct EpochMillisFn;
impl ScalarFunction for EpochMillisFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        Ok(Value::I64(ns / NANOS_PER_MILLI))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct EpochMicrosFn;
impl ScalarFunction for EpochMicrosFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        Ok(Value::I64(ns / NANOS_PER_MICRO))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Apply a fixed hour-offset timezone. Supports "UTC", "EST", "PST", etc. and "+HH:MM" / "-HH:MM".
fn tz_offset_nanos(tz: &str) -> Result<i64, String> {
    match tz.to_uppercase().as_str() {
        "UTC" | "GMT" => Ok(0),
        "EST" => Ok(-5 * NANOS_PER_HOUR),
        "EDT" => Ok(-4 * NANOS_PER_HOUR),
        "CST" => Ok(-6 * NANOS_PER_HOUR),
        "CDT" => Ok(-5 * NANOS_PER_HOUR),
        "MST" => Ok(-7 * NANOS_PER_HOUR),
        "MDT" => Ok(-6 * NANOS_PER_HOUR),
        "PST" => Ok(-8 * NANOS_PER_HOUR),
        "PDT" => Ok(-7 * NANOS_PER_HOUR),
        "CET" => Ok(1 * NANOS_PER_HOUR),
        "CEST" => Ok(2 * NANOS_PER_HOUR),
        "EET" => Ok(2 * NANOS_PER_HOUR),
        "EEST" => Ok(3 * NANOS_PER_HOUR),
        "JST" => Ok(9 * NANOS_PER_HOUR),
        "KST" => Ok(9 * NANOS_PER_HOUR),
        "CST_CN" | "HKT" | "SGT" => Ok(8 * NANOS_PER_HOUR),
        "IST" => Ok(5 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN),
        "AEST" => Ok(10 * NANOS_PER_HOUR),
        "AEDT" => Ok(11 * NANOS_PER_HOUR),
        "NZST" => Ok(12 * NANOS_PER_HOUR),
        "NZDT" => Ok(13 * NANOS_PER_HOUR),
        _ => {
            // Try +HH:MM or -HH:MM
            let s = tz.trim();
            if (s.starts_with('+') || s.starts_with('-')) && s.len() >= 3 {
                let sign: i64 = if s.starts_with('-') { -1 } else { 1 };
                let rest = &s[1..];
                let parts: Vec<&str> = rest.split(':').collect();
                let hours: i64 = parts[0].parse().map_err(|_| format!("invalid timezone: {tz}"))?;
                let mins: i64 = if parts.len() > 1 { parts[1].parse().unwrap_or(0) } else { 0 };
                Ok(sign * (hours * NANOS_PER_HOUR + mins * NANOS_PER_MIN))
            } else {
                Err(format!("unknown timezone: {tz}"))
            }
        }
    }
}

struct ToTimezoneFn;
impl ScalarFunction for ToTimezoneFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let tz = to_str(&args[1]);
        let offset = tz_offset_nanos(&tz)?;
        Ok(Value::Timestamp(ns + offset))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct FromUtcFn;
impl ScalarFunction for FromUtcFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let tz = to_str(&args[1]);
        let offset = tz_offset_nanos(&tz)?;
        Ok(Value::Timestamp(ns + offset))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct ToUtcFn;
impl ScalarFunction for ToUtcFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let tz = to_str(&args[1]);
        let offset = tz_offset_nanos(&tz)?;
        Ok(Value::Timestamp(ns - offset))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct DateFormatFn;
impl ScalarFunction for DateFormatFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let fmt = if args.len() > 1 { to_str(&args[1]) } else { "%Y-%m-%d %H:%M:%S".to_string() };
        let (year, month, day, hour, minute, second) = decompose_timestamp(ns);
        let result = fmt
            .replace("%Y", &format!("{:04}", year))
            .replace("%m", &format!("{:02}", month))
            .replace("%d", &format!("{:02}", day))
            .replace("%H", &format!("{:02}", hour))
            .replace("%M", &format!("{:02}", minute))
            .replace("%S", &format!("{:02}", second));
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

struct IsWeekendFn;
impl ScalarFunction for IsWeekendFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let total_secs = ns.div_euclid(NANOS_PER_SEC);
        let days = total_secs.div_euclid(86400);
        // day 0 (1970-01-01) was Thursday (4). 0=Mon..6=Sun
        let dow = ((days + 3) % 7 + 7) % 7; // 0=Mon, 6=Sun
        Ok(Value::I64(if dow >= 5 { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsBusinessDayFn;
impl ScalarFunction for IsBusinessDayFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let total_secs = ns.div_euclid(NANOS_PER_SEC);
        let days = total_secs.div_euclid(86400);
        let dow = ((days + 3) % 7 + 7) % 7;
        Ok(Value::I64(if dow < 5 { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct TimestampSequenceFn;
impl ScalarFunction for TimestampSequenceFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        // Returns the start timestamp (generator semantics not applicable to scalar context)
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let start = to_timestamp_ns(&args[0])?;
        Ok(Value::Timestamp(start))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

// ===========================================================================
// New string functions
// ===========================================================================

struct CharAtFn;
impl ScalarFunction for CharAtFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let idx = to_i64(&args[1])?;
        let idx = if idx > 0 { (idx - 1) as usize } else { return Ok(Value::Null); }; // 1-based to 0-based
        match s.chars().nth(idx) {
            Some(c) => Ok(Value::Str(c.to_string())),
            None => Ok(Value::Null),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct HexFn;
impl ScalarFunction for HexFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::I64(n) => Ok(Value::Str(format!("{:x}", n))),
            Value::F64(f) => Ok(Value::Str(format!("{:x}", *f as i64))),
            Value::Str(s) => {
                let hex: String = s.bytes().map(|b| format!("{:02x}", b)).collect();
                Ok(Value::Str(hex))
            }
            Value::Timestamp(ns) => Ok(Value::Str(format!("{:x}", ns))),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct UnhexFn;
impl ScalarFunction for UnhexFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let val = i64::from_str_radix(s.trim(), 16)
            .map_err(|e| format!("unhex: invalid hex string '{}': {}", s, e))?;
        Ok(Value::I64(val))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct UrlEncodeFn;
impl ScalarFunction for UrlEncodeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let mut encoded = String::with_capacity(s.len() * 3);
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    encoded.push(b as char);
                }
                _ => {
                    encoded.push_str(&format!("%{:02X}", b));
                }
            }
        }
        Ok(Value::Str(encoded))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct UrlDecodeFn;
impl ScalarFunction for UrlDecodeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let mut decoded = Vec::new();
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let hex = &s[i+1..i+3];
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    decoded.push(byte);
                    i += 3;
                    continue;
                }
            }
            if bytes[i] == b'+' {
                decoded.push(b' ');
            } else {
                decoded.push(bytes[i]);
            }
            i += 1;
        }
        Ok(Value::Str(String::from_utf8_lossy(&decoded).into_owned()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct JsonExtractFn;
impl ScalarFunction for JsonExtractFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let json_str = to_str(&args[0]);
        let path = to_str(&args[1]);
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| format!("json_extract: invalid JSON: {e}"))?;
        // Support simple dot-notation path: "key" or "key.subkey"
        let mut current = &parsed;
        for key in path.trim_start_matches('.').trim_start_matches('$').trim_start_matches('.').split('.') {
            let key = key.trim();
            if key.is_empty() { continue; }
            // Try array index
            if let Ok(idx) = key.parse::<usize>() {
                match current.get(idx) {
                    Some(v) => current = v,
                    None => return Ok(Value::Null),
                }
            } else {
                match current.get(key) {
                    Some(v) => current = v,
                    None => return Ok(Value::Null),
                }
            }
        }
        match current {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::I64(if *b { 1 } else { 0 })),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() { Ok(Value::I64(i)) }
                else if let Some(f) = n.as_f64() { Ok(Value::F64(f)) }
                else { Ok(Value::Str(n.to_string())) }
            }
            serde_json::Value::String(s) => Ok(Value::Str(s.clone())),
            other => Ok(Value::Str(other.to_string())),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct JsonArrayLengthFn;
impl ScalarFunction for JsonArrayLengthFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let json_str = to_str(&args[0]);
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| format!("json_array_length: invalid JSON: {e}"))?;
        match parsed {
            serde_json::Value::Array(arr) => Ok(Value::I64(arr.len() as i64)),
            _ => Err("json_array_length: expected JSON array".into()),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct RegexpCountFn;
impl ScalarFunction for RegexpCountFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let pattern = to_str(&args[1]);
        let re = Regex::new(&pattern).map_err(|e| format!("regexp_count: {e}"))?;
        Ok(Value::I64(re.find_iter(&s).count() as i64))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct RegexpSplitToArrayFn;
impl ScalarFunction for RegexpSplitToArrayFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let pattern = to_str(&args[1]);
        let re = Regex::new(&pattern).map_err(|e| format!("regexp_split_to_array: {e}"))?;
        let parts: Vec<&str> = re.split(&s).collect();
        Ok(Value::Str(format!("[{}]", parts.iter().map(|p| format!("\"{}\"", p)).collect::<Vec<_>>().join(","))))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct StringToArrayFn;
impl ScalarFunction for StringToArrayFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let sep = to_str(&args[1]);
        let parts: Vec<&str> = s.split(&sep).collect();
        Ok(Value::Str(format!("[{}]", parts.join(","))))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct ArrayToStringFn;
impl ScalarFunction for ArrayToStringFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let arr_str = to_str(&args[0]);
        let sep = to_str(&args[1]);
        // Parse simple "[a,b,c]" format
        let inner = arr_str.trim_start_matches('[').trim_end_matches(']');
        let parts: Vec<&str> = inner.split(',').collect();
        Ok(Value::Str(parts.iter().map(|s| s.trim().trim_matches('"')).collect::<Vec<_>>().join(&sep)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

// ===========================================================================
// New math functions
// ===========================================================================

struct ClampFn;
impl ScalarFunction for ClampFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        let min_val = to_f64(&args[1])?;
        let max_val = to_f64(&args[2])?;
        Ok(Value::F64(x.max(min_val).min(max_val)))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct LerpFn;
impl ScalarFunction for LerpFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args { if matches!(arg, Value::Null) { return Ok(Value::Null); } }
        let a = to_f64(&args[0])?;
        let b = to_f64(&args[1])?;
        let t = to_f64(&args[2])?;
        Ok(Value::F64(a + (b - a) * t))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct MapRangeFn;
impl ScalarFunction for MapRangeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args { if matches!(arg, Value::Null) { return Ok(Value::Null); } }
        let value = to_f64(&args[0])?;
        let in_min = to_f64(&args[1])?;
        let in_max = to_f64(&args[2])?;
        let out_min = to_f64(&args[3])?;
        let out_max = to_f64(&args[4])?;
        if (in_max - in_min).abs() < f64::EPSILON {
            return Err("map_range: input range is zero".into());
        }
        let t = (value - in_min) / (in_max - in_min);
        Ok(Value::F64(out_min + (out_max - out_min) * t))
    }
    fn min_args(&self) -> usize { 5 }
    fn max_args(&self) -> usize { 5 }
}

struct IsFiniteFn;
impl ScalarFunction for IsFiniteFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::F64(f) => Ok(Value::I64(if f.is_finite() { 1 } else { 0 })),
            Value::I64(_) => Ok(Value::I64(1)),
            Value::Null => Ok(Value::Null),
            _ => Ok(Value::I64(0)),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsNanFn;
impl ScalarFunction for IsNanFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::F64(f) => Ok(Value::I64(if f.is_nan() { 1 } else { 0 })),
            Value::Null => Ok(Value::Null),
            _ => Ok(Value::I64(0)),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsInfFn;
impl ScalarFunction for IsInfFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::F64(f) => Ok(Value::I64(if f.is_infinite() { 1 } else { 0 })),
            Value::Null => Ok(Value::Null),
            _ => Ok(Value::I64(0)),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct FmaFn;
impl ScalarFunction for FmaFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args { if matches!(arg, Value::Null) { return Ok(Value::Null); } }
        let x = to_f64(&args[0])?;
        let y = to_f64(&args[1])?;
        let z = to_f64(&args[2])?;
        Ok(Value::F64(x.mul_add(y, z)))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct HypotFn;
impl ScalarFunction for HypotFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args { if matches!(arg, Value::Null) { return Ok(Value::Null); } }
        let x = to_f64(&args[0])?;
        let y = to_f64(&args[1])?;
        Ok(Value::F64(x.hypot(y)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct CopysignFn;
impl ScalarFunction for CopysignFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args { if matches!(arg, Value::Null) { return Ok(Value::Null); } }
        let x = to_f64(&args[0])?;
        let y = to_f64(&args[1])?;
        Ok(Value::F64(x.copysign(y)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct NextPowerOfTwoFn;
impl ScalarFunction for NextPowerOfTwoFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let n = to_i64(&args[0])?;
        if n <= 0 { return Ok(Value::I64(1)); }
        Ok(Value::I64((n as u64).next_power_of_two() as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

// ===========================================================================
// Utility functions
// ===========================================================================

struct SizeofFn;
impl ScalarFunction for SizeofFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let size = match &args[0] {
            Value::Null => 0,
            Value::I64(_) => 8,
            Value::F64(_) => 8,
            Value::Timestamp(_) => 8,
            Value::Str(s) => s.len() as i64,
        };
        Ok(Value::I64(size))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct VersionFn;
impl ScalarFunction for VersionFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::Str("ExchangeDB 0.1.0".to_string()))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct PgTypeofFn;
impl ScalarFunction for PgTypeofFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let type_name = match &args[0] {
            Value::Null => "void",
            Value::I64(_) => "bigint",
            Value::F64(_) => "double precision",
            Value::Str(_) => "text",
            Value::Timestamp(_) => "timestamp",
        };
        Ok(Value::Str(type_name.to_string()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct HashFn;
impl ScalarFunction for HashFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match &args[0] {
            Value::Null => unreachable!(),
            Value::I64(v) => v.hash(&mut hasher),
            Value::F64(v) => v.to_bits().hash(&mut hasher),
            Value::Str(s) => s.hash(&mut hasher),
            Value::Timestamp(ns) => ns.hash(&mut hasher),
        }
        Ok(Value::I64(hasher.finish() as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct Murmur3Fn;
impl ScalarFunction for Murmur3Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        // Simple murmur3-like hash for 64-bit
        let s = to_str(&args[0]);
        let bytes = s.as_bytes();
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in bytes {
            h = h.wrapping_mul(0x100000001b3);
            h ^= b as u64;
        }
        // Mix
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        Ok(Value::I64(h as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct Crc32Fn;
impl ScalarFunction for Crc32Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        // CRC32 (IEEE) implementation
        let mut crc: u32 = 0xFFFFFFFF;
        for &byte in s.as_bytes() {
            crc ^= byte as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
        }
        Ok(Value::I64((crc ^ 0xFFFFFFFF) as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ToJsonFn;
impl ScalarFunction for ToJsonFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.len() == 1 {
            let json = match &args[0] {
                Value::Null => "null".to_string(),
                Value::I64(n) => n.to_string(),
                Value::F64(f) => f.to_string(),
                Value::Str(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
                Value::Timestamp(ns) => ns.to_string(),
            };
            return Ok(Value::Str(json));
        }
        // Multiple args -> JSON array
        let mut parts = Vec::new();
        for arg in args {
            let json = match arg {
                Value::Null => "null".to_string(),
                Value::I64(n) => n.to_string(),
                Value::F64(f) => f.to_string(),
                Value::Str(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
                Value::Timestamp(ns) => ns.to_string(),
            };
            parts.push(json);
        }
        Ok(Value::Str(format!("[{}]", parts.join(","))))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { usize::MAX }
}

struct TableNameFn;
impl ScalarFunction for TableNameFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if !args.is_empty() && matches!(args[0], Value::Null) { return Ok(Value::Null); }
        if !args.is_empty() {
            return Ok(args[0].clone());
        }
        Ok(Value::Str("unknown".to_string()))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 1 }
}

// ===========================================================================
// Extra compat/alias functions
// ===========================================================================

struct EConstFn;
impl ScalarFunction for EConstFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::F64(std::f64::consts::E))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct TauConstFn;
impl ScalarFunction for TauConstFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::F64(std::f64::consts::TAU))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct InfinityFn;
impl ScalarFunction for InfinityFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::F64(f64::INFINITY))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct NanFn;
impl ScalarFunction for NanFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::F64(f64::NAN))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct Nvl2Fn;
impl ScalarFunction for Nvl2Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        // nvl2(expr, val_if_not_null, val_if_null)
        if matches!(args[0], Value::Null) {
            Ok(args[2].clone())
        } else {
            Ok(args[1].clone())
        }
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct IifFn;
impl ScalarFunction for IifFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        // iif(condition, true_value, false_value)
        let cond = match &args[0] {
            Value::I64(v) => *v != 0,
            Value::F64(v) => *v != 0.0,
            Value::Str(s) => !s.is_empty() && s != "0" && s.to_ascii_lowercase() != "false",
            Value::Null => false,
            Value::Timestamp(_) => true,
        };
        Ok(if cond { args[1].clone() } else { args[2].clone() })
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

struct DecodeCaseFn;
impl ScalarFunction for DecodeCaseFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        // decode_fn(expr, val1, result1, val2, result2, ..., default)
        if args.len() < 3 { return Err("decode_fn requires at least 3 arguments".into()); }
        let expr = &args[0];
        let mut i = 1;
        while i + 1 < args.len() {
            if expr == &args[i] { return Ok(args[i + 1].clone()); }
            i += 2;
        }
        // Default (last arg if odd number of remaining args)
        if i < args.len() { Ok(args[i].clone()) } else { Ok(Value::Null) }
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { usize::MAX }
}

struct SwitchFn;
impl ScalarFunction for SwitchFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        // switch(expr, val1, result1, ..., default) - same as decode
        if args.len() < 3 { return Err("switch requires at least 3 arguments".into()); }
        let expr = &args[0];
        let mut i = 1;
        while i + 1 < args.len() {
            if expr == &args[i] { return Ok(args[i + 1].clone()); }
            i += 2;
        }
        if i < args.len() { Ok(args[i].clone()) } else { Ok(Value::Null) }
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { usize::MAX }
}

struct LogBaseFn;
impl ScalarFunction for LogBaseFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args { if matches!(arg, Value::Null) { return Ok(Value::Null); } }
        let base = to_f64(&args[0])?;
        let x = to_f64(&args[1])?;
        if base <= 0.0 || base == 1.0 { return Err("log_base: base must be positive and != 1".into()); }
        if x <= 0.0 { return Err("log_base: argument must be positive".into()); }
        Ok(Value::F64(x.log(base)))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct SquareFn;
impl ScalarFunction for SquareFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        Ok(Value::F64(x * x))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct NegateFn;
impl ScalarFunction for NegateFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::I64(n) => Ok(Value::I64(-n)),
            Value::F64(f) => Ok(Value::F64(-f)),
            Value::Null => Ok(Value::Null),
            other => Err(format!("negate: expected numeric, got {other}")),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ReciprocalFn;
impl ScalarFunction for ReciprocalFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        if x == 0.0 { return Err("reciprocal: division by zero".into()); }
        Ok(Value::F64(1.0 / x))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct Fnv1aFn;
impl ScalarFunction for Fnv1aFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in s.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        Ok(Value::I64(h as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct StrcmpFn;
impl ScalarFunction for StrcmpFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.iter().any(|a| matches!(a, Value::Null)) { return Ok(Value::Null); }
        let a = to_str(&args[0]);
        let b = to_str(&args[1]);
        Ok(Value::I64(match a.cmp(&b) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct SoundexFn;
impl ScalarFunction for SoundexFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]).to_uppercase();
        let mut result = String::new();
        for (i, ch) in s.chars().enumerate() {
            if i == 0 {
                result.push(ch);
                continue;
            }
            let code = match ch {
                'B' | 'F' | 'P' | 'V' => '1',
                'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
                'D' | 'T' => '3',
                'L' => '4',
                'M' | 'N' => '5',
                'R' => '6',
                _ => continue,
            };
            if result.len() < 4 && result.chars().last() != Some(code) {
                result.push(code);
            }
        }
        while result.len() < 4 { result.push('0'); }
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct SpaceFn;
impl ScalarFunction for SpaceFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let n = to_i64(&args[0])?.max(0) as usize;
        Ok(Value::Str(" ".repeat(n)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ToBase64Fn;
impl ScalarFunction for ToBase64Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        // Simple base64 encode
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = s.as_bytes();
        let mut result = String::new();
        let mut i = 0;
        while i < bytes.len() {
            let b0 = bytes[i] as u32;
            let b1 = if i + 1 < bytes.len() { bytes[i + 1] as u32 } else { 0 };
            let b2 = if i + 2 < bytes.len() { bytes[i + 2] as u32 } else { 0 };
            let triple = (b0 << 16) | (b1 << 8) | b2;
            result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
            result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
            if i + 1 < bytes.len() { result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char); } else { result.push('='); }
            if i + 2 < bytes.len() { result.push(CHARS[(triple & 0x3F) as usize] as char); } else { result.push('='); }
            i += 3;
        }
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct FromBase64Fn;
impl ScalarFunction for FromBase64Fn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(|e| format!("base64 decode error: {e}"))?;
        Ok(Value::Str(String::from_utf8_lossy(&decoded).into_owned()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct WordCountFn;
impl ScalarFunction for WordCountFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        Ok(Value::I64(s.split_whitespace().count() as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CamelCaseFn;
impl ScalarFunction for CamelCaseFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let mut result = String::new();
        let mut capitalize_next = false;
        for (i, ch) in s.chars().enumerate() {
            if ch == '_' || ch == ' ' || ch == '-' {
                capitalize_next = true;
            } else if capitalize_next || i == 0 {
                result.extend(ch.to_uppercase());
                capitalize_next = false;
            } else {
                result.push(ch);
            }
        }
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct SnakeCaseFn;
impl ScalarFunction for SnakeCaseFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let mut result = String::new();
        for (i, ch) in s.chars().enumerate() {
            if ch.is_uppercase() && i > 0 {
                result.push('_');
            }
            result.extend(ch.to_lowercase());
        }
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct SqueezeFn;
impl ScalarFunction for SqueezeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let mut result = String::new();
        let mut prev_space = false;
        for ch in s.chars() {
            if ch.is_whitespace() {
                if !prev_space { result.push(' '); }
                prev_space = true;
            } else {
                result.push(ch);
                prev_space = false;
            }
        }
        Ok(Value::Str(result.trim().to_string()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct CountCharFn;
impl ScalarFunction for CountCharFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let s = to_str(&args[0]);
        let ch_str = to_str(&args[1]);
        let ch = ch_str.chars().next().unwrap_or('\0');
        Ok(Value::I64(s.chars().filter(|&c| c == ch).count() as i64))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

// ===========================================================================
// Date boundary functions
// ===========================================================================

struct StartOfYearFn;
impl ScalarFunction for StartOfYearFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, _, _, _, _, _) = decompose_timestamp(ns);
        Ok(Value::Timestamp(civil_to_days(year, 1, 1) * NANOS_PER_DAY))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct EndOfYearFn;
impl ScalarFunction for EndOfYearFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, _, _, _, _, _) = decompose_timestamp(ns);
        Ok(Value::Timestamp(civil_to_days(year, 12, 31) * NANOS_PER_DAY))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct StartOfQuarterFn;
impl ScalarFunction for StartOfQuarterFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let (year, month, _, _, _, _) = decompose_timestamp(ns);
        let q_month = ((month - 1) / 3) * 3 + 1;
        Ok(Value::Timestamp(civil_to_days(year, q_month, 1) * NANOS_PER_DAY))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct StartOfWeekFn;
impl ScalarFunction for StartOfWeekFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let ns = to_timestamp_ns(&args[0])?;
        let total_secs = ns.div_euclid(NANOS_PER_SEC);
        let days = total_secs.div_euclid(86400);
        // Monday-based week: day 0 (1970-01-01) was Thursday (3 from Monday)
        let dow = ((days + 3) % 7 + 7) % 7; // 0=Mon
        let monday = days - dow;
        Ok(Value::Timestamp(monday * NANOS_PER_DAY))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

// ===========================================================================
// Numeric predicate functions
// ===========================================================================

struct IsPositiveFn;
impl ScalarFunction for IsPositiveFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        Ok(Value::I64(if x > 0.0 { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsNegativeFn;
impl ScalarFunction for IsNegativeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        Ok(Value::I64(if x < 0.0 { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsZeroFn;
impl ScalarFunction for IsZeroFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        Ok(Value::I64(if x == 0.0 { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsEvenFn;
impl ScalarFunction for IsEvenFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let n = to_i64(&args[0])?;
        Ok(Value::I64(if n % 2 == 0 { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct IsOddFn;
impl ScalarFunction for IsOddFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let n = to_i64(&args[0])?;
        Ok(Value::I64(if n % 2 != 0 { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct BetweenFn;
impl ScalarFunction for BetweenFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let x = to_f64(&args[0])?;
        let lo = to_f64(&args[1])?;
        let hi = to_f64(&args[2])?;
        Ok(Value::I64(if x >= lo && x <= hi { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 3 }
    fn max_args(&self) -> usize { 3 }
}

// ===========================================================================
// Misc utility functions
// ===========================================================================

struct RowNumberFn;
impl ScalarFunction for RowNumberFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        use std::sync::atomic::{AtomicI64, Ordering};
        static ROW_COUNTER: AtomicI64 = AtomicI64::new(0);
        Ok(Value::I64(ROW_COUNTER.fetch_add(1, Ordering::Relaxed) + 1))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct HashCombineFn;
impl ScalarFunction for HashCombineFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args { if matches!(arg, Value::Null) { return Ok(Value::Null); } }
        let a = to_i64(&args[0])? as u64;
        let b = to_i64(&args[1])? as u64;
        // Boost-style hash combine
        let combined = a ^ (b.wrapping_add(0x9e3779b9).wrapping_add(a << 6).wrapping_add(a >> 2));
        Ok(Value::I64(combined as i64))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct ZeroIfNullFn;
impl ScalarFunction for ZeroIfNullFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::Null => Ok(Value::I64(0)),
            other => Ok(other.clone()),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ConcatWsFn;
impl ScalarFunction for ConcatWsFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.is_empty() { return Err("concat_ws requires at least 1 argument".into()); }
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let sep = to_str(&args[0]);
        let parts: Vec<String> = args[1..].iter()
            .filter(|a| !matches!(a, Value::Null))
            .map(|a| to_str(a))
            .collect();
        Ok(Value::Str(parts.join(&sep)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { usize::MAX }
}

struct BitCountFn;
impl ScalarFunction for BitCountFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let n = to_i64(&args[0])?;
        Ok(Value::I64((n as u64).count_ones() as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct LeadingZerosFn;
impl ScalarFunction for LeadingZerosFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let n = to_i64(&args[0])?;
        Ok(Value::I64((n as u64).leading_zeros() as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct TrailingZerosFn;
impl ScalarFunction for TrailingZerosFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        let n = to_i64(&args[0])?;
        if n == 0 { return Ok(Value::I64(64)); }
        Ok(Value::I64((n as u64).trailing_zeros() as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct ByteLengthFn;
impl ScalarFunction for ByteLengthFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::I64(to_str(&args[0]).len() as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct BitLengthFn;
impl ScalarFunction for BitLengthFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::I64((to_str(&args[0]).len() * 8) as i64))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct YesterdayFn;
impl ScalarFunction for YesterdayFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        let ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        Ok(Value::Timestamp(ns - NANOS_PER_DAY))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct TomorrowFn;
impl ScalarFunction for TomorrowFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        let ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        Ok(Value::Timestamp(ns + NANOS_PER_DAY))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct SafeCastIntFn;
impl ScalarFunction for SafeCastIntFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::I64(n) => Ok(Value::I64(*n)),
            Value::F64(f) => Ok(Value::I64(*f as i64)),
            Value::Str(s) => Ok(s.parse::<i64>().map(Value::I64).unwrap_or(Value::Null)),
            Value::Null => Ok(Value::Null),
            Value::Timestamp(ns) => Ok(Value::I64(*ns)),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct SafeCastFloatFn;
impl ScalarFunction for SafeCastFloatFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::F64(f) => Ok(Value::F64(*f)),
            Value::I64(n) => Ok(Value::F64(*n as f64)),
            Value::Str(s) => Ok(s.parse::<f64>().map(Value::F64).unwrap_or(Value::Null)),
            Value::Null => Ok(Value::Null),
            Value::Timestamp(ns) => Ok(Value::F64(*ns as f64)),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

struct AbsDiffFn;
impl ScalarFunction for AbsDiffFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        for arg in args { if matches!(arg, Value::Null) { return Ok(Value::Null); } }
        let a = to_f64(&args[0])?;
        let b = to_f64(&args[1])?;
        Ok(Value::F64((a - b).abs()))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

struct CurrentSchemaFn;
impl ScalarFunction for CurrentSchemaFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::Str("public".to_string()))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct CurrentDatabaseFn;
impl ScalarFunction for CurrentDatabaseFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::Str("exchangedb".to_string()))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct CurrentUserFn;
impl ScalarFunction for CurrentUserFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::Str("admin".to_string()))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

struct NullIfEmptyFn;
impl ScalarFunction for NullIfEmptyFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::Str(s) if s.is_empty() => Ok(Value::Null),
            Value::Null => Ok(Value::Null),
            other => Ok(other.clone()),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_length() {
        assert_eq!(
            evaluate_scalar("length", &[Value::Str("hello".into())]).unwrap(),
            Value::I64(5)
        );
        assert_eq!(
            evaluate_scalar("length", &[Value::Null]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_upper_lower() {
        assert_eq!(
            evaluate_scalar("upper", &[Value::Str("hello".into())]).unwrap(),
            Value::Str("HELLO".into())
        );
        assert_eq!(
            evaluate_scalar("lower", &[Value::Str("HELLO".into())]).unwrap(),
            Value::Str("hello".into())
        );
    }

    #[test]
    fn test_trim() {
        assert_eq!(
            evaluate_scalar("trim", &[Value::Str("  hi  ".into())]).unwrap(),
            Value::Str("hi".into())
        );
        assert_eq!(
            evaluate_scalar("ltrim", &[Value::Str("  hi  ".into())]).unwrap(),
            Value::Str("hi  ".into())
        );
        assert_eq!(
            evaluate_scalar("rtrim", &[Value::Str("  hi  ".into())]).unwrap(),
            Value::Str("  hi".into())
        );
    }

    #[test]
    fn test_substring() {
        assert_eq!(
            evaluate_scalar("substring", &[
                Value::Str("hello world".into()),
                Value::I64(1),
                Value::I64(5),
            ]).unwrap(),
            Value::Str("hello".into())
        );
        assert_eq!(
            evaluate_scalar("substring", &[
                Value::Str("hello world".into()),
                Value::I64(7),
                Value::I64(5),
            ]).unwrap(),
            Value::Str("world".into())
        );
    }

    #[test]
    fn test_concat() {
        assert_eq!(
            evaluate_scalar("concat", &[
                Value::Str("hello".into()),
                Value::Str(" world".into()),
            ]).unwrap(),
            Value::Str("hello world".into())
        );
    }

    #[test]
    fn test_replace() {
        assert_eq!(
            evaluate_scalar("replace", &[
                Value::Str("hello world".into()),
                Value::Str("world".into()),
                Value::Str("rust".into()),
            ]).unwrap(),
            Value::Str("hello rust".into())
        );
    }

    #[test]
    fn test_starts_ends_contains() {
        assert_eq!(
            evaluate_scalar("starts_with", &[
                Value::Str("hello".into()),
                Value::Str("hel".into()),
            ]).unwrap(),
            Value::I64(1)
        );
        assert_eq!(
            evaluate_scalar("ends_with", &[
                Value::Str("hello".into()),
                Value::Str("llo".into()),
            ]).unwrap(),
            Value::I64(1)
        );
        assert_eq!(
            evaluate_scalar("contains", &[
                Value::Str("hello".into()),
                Value::Str("ell".into()),
            ]).unwrap(),
            Value::I64(1)
        );
        assert_eq!(
            evaluate_scalar("contains", &[
                Value::Str("hello".into()),
                Value::Str("xyz".into()),
            ]).unwrap(),
            Value::I64(0)
        );
    }

    #[test]
    fn test_reverse() {
        assert_eq!(
            evaluate_scalar("reverse", &[Value::Str("abcde".into())]).unwrap(),
            Value::Str("edcba".into())
        );
    }

    #[test]
    fn test_repeat() {
        assert_eq!(
            evaluate_scalar("repeat", &[Value::Str("ab".into()), Value::I64(3)]).unwrap(),
            Value::Str("ababab".into())
        );
    }

    #[test]
    fn test_left_right() {
        assert_eq!(
            evaluate_scalar("left", &[Value::Str("hello".into()), Value::I64(3)]).unwrap(),
            Value::Str("hel".into())
        );
        assert_eq!(
            evaluate_scalar("right", &[Value::Str("hello".into()), Value::I64(3)]).unwrap(),
            Value::Str("llo".into())
        );
    }

    #[test]
    fn test_abs() {
        assert_eq!(
            evaluate_scalar("abs", &[Value::I64(-42)]).unwrap(),
            Value::I64(42)
        );
        assert_eq!(
            evaluate_scalar("abs", &[Value::F64(-3.14)]).unwrap(),
            Value::F64(3.14)
        );
    }

    #[test]
    fn test_round() {
        assert_eq!(
            evaluate_scalar("round", &[Value::F64(3.14159), Value::I64(2)]).unwrap(),
            Value::F64(3.14)
        );
        assert_eq!(
            evaluate_scalar("round", &[Value::F64(3.5)]).unwrap(),
            Value::F64(4.0)
        );
    }

    #[test]
    fn test_floor_ceil() {
        assert_eq!(
            evaluate_scalar("floor", &[Value::F64(3.7)]).unwrap(),
            Value::F64(3.0)
        );
        assert_eq!(
            evaluate_scalar("ceil", &[Value::F64(3.2)]).unwrap(),
            Value::F64(4.0)
        );
    }

    #[test]
    fn test_sqrt() {
        assert_eq!(
            evaluate_scalar("sqrt", &[Value::F64(9.0)]).unwrap(),
            Value::F64(3.0)
        );
        assert!(evaluate_scalar("sqrt", &[Value::F64(-1.0)]).is_err());
    }

    #[test]
    fn test_pow() {
        assert_eq!(
            evaluate_scalar("pow", &[Value::F64(2.0), Value::F64(3.0)]).unwrap(),
            Value::F64(8.0)
        );
    }

    #[test]
    fn test_log_exp() {
        let result = evaluate_scalar("exp", &[Value::F64(1.0)]).unwrap();
        if let Value::F64(v) = result {
            assert!((v - std::f64::consts::E).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }

        let result = evaluate_scalar("log", &[Value::F64(std::f64::consts::E)]).unwrap();
        if let Value::F64(v) = result {
            assert!((v - 1.0).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_trig() {
        let result = evaluate_scalar("sin", &[Value::F64(0.0)]).unwrap();
        assert_eq!(result, Value::F64(0.0));

        let result = evaluate_scalar("cos", &[Value::F64(0.0)]).unwrap();
        assert_eq!(result, Value::F64(1.0));
    }

    #[test]
    fn test_mod_fn() {
        assert_eq!(
            evaluate_scalar("mod", &[Value::I64(10), Value::I64(3)]).unwrap(),
            Value::I64(1)
        );
    }

    #[test]
    fn test_sign() {
        assert_eq!(
            evaluate_scalar("sign", &[Value::I64(42)]).unwrap(),
            Value::I64(1)
        );
        assert_eq!(
            evaluate_scalar("sign", &[Value::I64(-5)]).unwrap(),
            Value::I64(-1)
        );
        assert_eq!(
            evaluate_scalar("sign", &[Value::I64(0)]).unwrap(),
            Value::I64(0)
        );
    }

    #[test]
    fn test_pi() {
        assert_eq!(
            evaluate_scalar("pi", &[]).unwrap(),
            Value::F64(std::f64::consts::PI)
        );
    }

    #[test]
    fn test_random() {
        let result = evaluate_scalar("random", &[]).unwrap();
        if let Value::F64(v) = result {
            assert!(v >= 0.0 && v <= 1.0);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_now() {
        let result = evaluate_scalar("now", &[]).unwrap();
        assert!(matches!(result, Value::Timestamp(_)));
    }

    #[test]
    fn test_to_timestamp() {
        assert_eq!(
            evaluate_scalar("to_timestamp", &[Value::I64(1_000_000_000)]).unwrap(),
            Value::Timestamp(1_000_000_000)
        );
    }

    #[test]
    fn test_extract_year_month_day_hour() {
        // 2024-01-15 10:30:00 UTC in nanoseconds
        // Jan 15, 2024 = day 19737 from epoch
        let ns = 19737_i64 * NANOS_PER_DAY + 10 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN;
        let ts = Value::Timestamp(ns);

        assert_eq!(
            evaluate_scalar("extract_year", &[ts.clone()]).unwrap(),
            Value::I64(2024)
        );
        assert_eq!(
            evaluate_scalar("extract_month", &[ts.clone()]).unwrap(),
            Value::I64(1)
        );
        assert_eq!(
            evaluate_scalar("extract_day", &[ts.clone()]).unwrap(),
            Value::I64(15)
        );
        assert_eq!(
            evaluate_scalar("extract_hour", &[ts]).unwrap(),
            Value::I64(10)
        );
    }

    #[test]
    fn test_date_trunc() {
        // Some timestamp with sub-day precision
        let ns = 19737_i64 * NANOS_PER_DAY + 10 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN + 45 * NANOS_PER_SEC;

        // Truncate to day
        let result = evaluate_scalar("date_trunc", &[
            Value::Str("day".into()),
            Value::Timestamp(ns),
        ]).unwrap();
        assert_eq!(result, Value::Timestamp(19737 * NANOS_PER_DAY));

        // Truncate to hour
        let result = evaluate_scalar("date_trunc", &[
            Value::Str("hour".into()),
            Value::Timestamp(ns),
        ]).unwrap();
        assert_eq!(result, Value::Timestamp(19737 * NANOS_PER_DAY + 10 * NANOS_PER_HOUR));
    }

    #[test]
    fn test_date_diff() {
        let ts1 = Value::Timestamp(0);
        let ts2 = Value::Timestamp(3 * NANOS_PER_HOUR);
        assert_eq!(
            evaluate_scalar("date_diff", &[
                Value::Str("hour".into()),
                ts1,
                ts2,
            ]).unwrap(),
            Value::I64(3)
        );
    }

    #[test]
    fn test_timestamp_add() {
        let ts = Value::Timestamp(0);
        let result = evaluate_scalar("timestamp_add", &[
            Value::Str("day".into()),
            Value::I64(1),
            ts,
        ]).unwrap();
        assert_eq!(result, Value::Timestamp(NANOS_PER_DAY));
    }

    #[test]
    fn test_epoch_nanos() {
        let ns = 123_456_789_i64;
        assert_eq!(
            evaluate_scalar("epoch_nanos", &[Value::Timestamp(ns)]).unwrap(),
            Value::I64(ns)
        );
    }

    #[test]
    fn test_coalesce() {
        assert_eq!(
            evaluate_scalar("coalesce", &[Value::Null, Value::I64(42)]).unwrap(),
            Value::I64(42)
        );
        assert_eq!(
            evaluate_scalar("coalesce", &[Value::Null, Value::Null, Value::Str("x".into())]).unwrap(),
            Value::Str("x".into())
        );
    }

    #[test]
    fn test_nullif() {
        assert_eq!(
            evaluate_scalar("nullif", &[Value::I64(1), Value::I64(1)]).unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar("nullif", &[Value::I64(1), Value::I64(2)]).unwrap(),
            Value::I64(1)
        );
    }

    #[test]
    fn test_greatest_least() {
        assert_eq!(
            evaluate_scalar("greatest", &[Value::I64(1), Value::I64(5), Value::I64(3)]).unwrap(),
            Value::I64(5)
        );
        assert_eq!(
            evaluate_scalar("least", &[Value::I64(1), Value::I64(5), Value::I64(3)]).unwrap(),
            Value::I64(1)
        );
    }

    #[test]
    fn test_if_null() {
        assert_eq!(
            evaluate_scalar("if_null", &[Value::Null, Value::I64(99)]).unwrap(),
            Value::I64(99)
        );
        assert_eq!(
            evaluate_scalar("if_null", &[Value::I64(42), Value::I64(99)]).unwrap(),
            Value::I64(42)
        );
    }

    #[test]
    fn test_log2_log10() {
        let result = evaluate_scalar("log2", &[Value::F64(8.0)]).unwrap();
        if let Value::F64(v) = result {
            assert!((v - 3.0).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }

        let result = evaluate_scalar("log10", &[Value::F64(1000.0)]).unwrap();
        if let Value::F64(v) = result {
            assert!((v - 3.0).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_registry_case_insensitive() {
        let reg = ScalarRegistry::new();
        assert!(reg.get("LENGTH").is_some());
        assert!(reg.get("Upper").is_some());
        assert!(reg.get("pi").is_some());
        assert!(reg.get("nonexistent").is_none());
    }

    // -----------------------------------------------------------------------
    // Tests for new string functions
    // -----------------------------------------------------------------------

    #[test]
    fn test_lpad_rpad() {
        assert_eq!(
            evaluate_scalar("lpad", &[Value::Str("hi".into()), Value::I64(5), Value::Str("xy".into())]).unwrap(),
            Value::Str("xyxhi".into())
        );
        assert_eq!(
            evaluate_scalar("rpad", &[Value::Str("hi".into()), Value::I64(5), Value::Str("xy".into())]).unwrap(),
            Value::Str("hixyx".into())
        );
        // Truncation when string is longer than len
        assert_eq!(
            evaluate_scalar("lpad", &[Value::Str("hello".into()), Value::I64(3), Value::Str("x".into())]).unwrap(),
            Value::Str("hel".into())
        );
    }

    #[test]
    fn test_split_part() {
        assert_eq!(
            evaluate_scalar("split_part", &[
                Value::Str("a.b.c".into()),
                Value::Str(".".into()),
                Value::I64(2),
            ]).unwrap(),
            Value::Str("b".into())
        );
        assert_eq!(
            evaluate_scalar("split_part", &[
                Value::Str("a.b.c".into()),
                Value::Str(".".into()),
                Value::I64(5),
            ]).unwrap(),
            Value::Str("".into())
        );
    }

    #[test]
    fn test_regexp_match() {
        assert_eq!(
            evaluate_scalar("regexp_match", &[
                Value::Str("hello123".into()),
                Value::Str(r"\d+".into()),
            ]).unwrap(),
            Value::I64(1)
        );
        assert_eq!(
            evaluate_scalar("regexp_match", &[
                Value::Str("hello".into()),
                Value::Str(r"\d+".into()),
            ]).unwrap(),
            Value::I64(0)
        );
    }

    #[test]
    fn test_regexp_replace() {
        assert_eq!(
            evaluate_scalar("regexp_replace", &[
                Value::Str("hello 123 world 456".into()),
                Value::Str(r"\d+".into()),
                Value::Str("NUM".into()),
            ]).unwrap(),
            Value::Str("hello NUM world NUM".into())
        );
    }

    #[test]
    fn test_regexp_extract() {
        assert_eq!(
            evaluate_scalar("regexp_extract", &[
                Value::Str("price: $42.50".into()),
                Value::Str(r"\$(\d+\.\d+)".into()),
                Value::I64(1),
            ]).unwrap(),
            Value::Str("42.50".into())
        );
        // No match
        assert_eq!(
            evaluate_scalar("regexp_extract", &[
                Value::Str("no numbers".into()),
                Value::Str(r"(\d+)".into()),
                Value::I64(1),
            ]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_md5() {
        assert_eq!(
            evaluate_scalar("md5", &[Value::Str("".into())]).unwrap(),
            Value::Str("d41d8cd98f00b204e9800998ecf8427e".into())
        );
        assert_eq!(
            evaluate_scalar("md5", &[Value::Str("hello".into())]).unwrap(),
            Value::Str("5d41402abc4b2a76b9719d911017c592".into())
        );
    }

    #[test]
    fn test_sha256() {
        assert_eq!(
            evaluate_scalar("sha256", &[Value::Str("".into())]).unwrap(),
            Value::Str("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into())
        );
        assert_eq!(
            evaluate_scalar("sha256", &[Value::Str("hello".into())]).unwrap(),
            Value::Str("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into())
        );
    }

    #[test]
    fn test_initcap() {
        assert_eq!(
            evaluate_scalar("initcap", &[Value::Str("hello world foo".into())]).unwrap(),
            Value::Str("Hello World Foo".into())
        );
    }

    #[test]
    fn test_position() {
        assert_eq!(
            evaluate_scalar("position", &[Value::Str("lo".into()), Value::Str("hello".into())]).unwrap(),
            Value::I64(4)
        );
        assert_eq!(
            evaluate_scalar("position", &[Value::Str("xyz".into()), Value::Str("hello".into())]).unwrap(),
            Value::I64(0)
        );
    }

    #[test]
    fn test_overlay() {
        assert_eq!(
            evaluate_scalar("overlay", &[
                Value::Str("hello world".into()),
                Value::Str("RUST".into()),
                Value::I64(7),
                Value::I64(5),
            ]).unwrap(),
            Value::Str("hello RUST".into())
        );
    }

    #[test]
    fn test_translate() {
        assert_eq!(
            evaluate_scalar("translate", &[
                Value::Str("hello".into()),
                Value::Str("helo".into()),
                Value::Str("HELO".into()),
            ]).unwrap(),
            Value::Str("HELLO".into())
        );
    }

    #[test]
    fn test_encode_decode_base64() {
        assert_eq!(
            evaluate_scalar("encode", &[Value::Str("hello".into()), Value::Str("base64".into())]).unwrap(),
            Value::Str("aGVsbG8=".into())
        );
        assert_eq!(
            evaluate_scalar("decode", &[Value::Str("aGVsbG8=".into()), Value::Str("base64".into())]).unwrap(),
            Value::Str("hello".into())
        );
    }

    #[test]
    fn test_ascii_chr() {
        assert_eq!(
            evaluate_scalar("ascii", &[Value::Str("A".into())]).unwrap(),
            Value::I64(65)
        );
        assert_eq!(
            evaluate_scalar("chr", &[Value::I64(65)]).unwrap(),
            Value::Str("A".into())
        );
    }

    #[test]
    fn test_quote_ident_literal() {
        assert_eq!(
            evaluate_scalar("quote_ident", &[Value::Str("my column".into())]).unwrap(),
            Value::Str("\"my column\"".into())
        );
        assert_eq!(
            evaluate_scalar("quote_literal", &[Value::Str("it's".into())]).unwrap(),
            Value::Str("'it''s'".into())
        );
    }

    #[test]
    fn test_format() {
        assert_eq!(
            evaluate_scalar("format", &[
                Value::Str("Hello %s, you have %s items".into()),
                Value::Str("Alice".into()),
                Value::I64(5),
            ]).unwrap(),
            Value::Str("Hello Alice, you have 5 items".into())
        );
    }

    // -----------------------------------------------------------------------
    // Tests for type casting functions
    // -----------------------------------------------------------------------

    #[test]
    fn test_cast_int() {
        assert_eq!(
            evaluate_scalar("cast_int", &[Value::F64(3.7)]).unwrap(),
            Value::I64(3)
        );
        assert_eq!(
            evaluate_scalar("to_int", &[Value::Str("42".into())]).unwrap(),
            Value::I64(42)
        );
    }

    #[test]
    fn test_typeof() {
        assert_eq!(
            evaluate_scalar("typeof", &[Value::I64(1)]).unwrap(),
            Value::Str("i64".into())
        );
        assert_eq!(
            evaluate_scalar("typeof", &[Value::Str("hi".into())]).unwrap(),
            Value::Str("string".into())
        );
        assert_eq!(
            evaluate_scalar("typeof", &[Value::Null]).unwrap(),
            Value::Str("null".into())
        );
    }

    #[test]
    fn test_is_null_is_not_null() {
        assert_eq!(evaluate_scalar("is_null", &[Value::Null]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("is_null", &[Value::I64(1)]).unwrap(), Value::I64(0));
        assert_eq!(evaluate_scalar("is_not_null", &[Value::Null]).unwrap(), Value::I64(0));
        assert_eq!(evaluate_scalar("is_not_null", &[Value::I64(1)]).unwrap(), Value::I64(1));
    }

    #[test]
    fn test_nullif_zero() {
        assert_eq!(evaluate_scalar("nullif_zero", &[Value::I64(0)]).unwrap(), Value::Null);
        assert_eq!(evaluate_scalar("nullif_zero", &[Value::I64(5)]).unwrap(), Value::I64(5));
        assert_eq!(evaluate_scalar("nullif_zero", &[Value::F64(0.0)]).unwrap(), Value::Null);
    }

    // -----------------------------------------------------------------------
    // Tests for additional math functions
    // -----------------------------------------------------------------------

    #[test]
    fn test_degrees_radians() {
        let result = evaluate_scalar("degrees", &[Value::F64(std::f64::consts::PI)]).unwrap();
        if let Value::F64(v) = result {
            assert!((v - 180.0).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }

        let result = evaluate_scalar("radians", &[Value::F64(180.0)]).unwrap();
        if let Value::F64(v) = result {
            assert!((v - std::f64::consts::PI).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_inverse_trig() {
        let result = evaluate_scalar("asin", &[Value::F64(1.0)]).unwrap();
        if let Value::F64(v) = result {
            assert!((v - std::f64::consts::FRAC_PI_2).abs() < 1e-10);
        } else { panic!("expected F64"); }

        let result = evaluate_scalar("acos", &[Value::F64(1.0)]).unwrap();
        if let Value::F64(v) = result {
            assert!(v.abs() < 1e-10);
        } else { panic!("expected F64"); }

        let result = evaluate_scalar("atan", &[Value::F64(0.0)]).unwrap();
        assert_eq!(result, Value::F64(0.0));
    }

    #[test]
    fn test_cbrt() {
        assert_eq!(
            evaluate_scalar("cbrt", &[Value::F64(27.0)]).unwrap(),
            Value::F64(3.0)
        );
    }

    #[test]
    fn test_factorial() {
        assert_eq!(evaluate_scalar("factorial", &[Value::I64(0)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("factorial", &[Value::I64(5)]).unwrap(), Value::I64(120));
        assert_eq!(evaluate_scalar("factorial", &[Value::I64(10)]).unwrap(), Value::I64(3628800));
        assert!(evaluate_scalar("factorial", &[Value::I64(-1)]).is_err());
    }

    #[test]
    fn test_gcd_lcm() {
        assert_eq!(evaluate_scalar("gcd", &[Value::I64(12), Value::I64(8)]).unwrap(), Value::I64(4));
        assert_eq!(evaluate_scalar("lcm", &[Value::I64(4), Value::I64(6)]).unwrap(), Value::I64(12));
    }

    #[test]
    fn test_bitwise() {
        assert_eq!(evaluate_scalar("bit_and", &[Value::I64(0b1100), Value::I64(0b1010)]).unwrap(), Value::I64(0b1000));
        assert_eq!(evaluate_scalar("bit_or", &[Value::I64(0b1100), Value::I64(0b1010)]).unwrap(), Value::I64(0b1110));
        assert_eq!(evaluate_scalar("bit_xor", &[Value::I64(0b1100), Value::I64(0b1010)]).unwrap(), Value::I64(0b0110));
        assert_eq!(evaluate_scalar("bit_shift_left", &[Value::I64(1), Value::I64(4)]).unwrap(), Value::I64(16));
        assert_eq!(evaluate_scalar("bit_shift_right", &[Value::I64(16), Value::I64(2)]).unwrap(), Value::I64(4));
    }

    #[test]
    fn test_trunc() {
        assert_eq!(
            evaluate_scalar("trunc", &[Value::F64(3.14159), Value::I64(2)]).unwrap(),
            Value::F64(3.14)
        );
        assert_eq!(
            evaluate_scalar("trunc", &[Value::F64(-3.7)]).unwrap(),
            Value::F64(-3.0)
        );
    }

    #[test]
    fn test_div() {
        assert_eq!(evaluate_scalar("div", &[Value::I64(10), Value::I64(3)]).unwrap(), Value::I64(3));
        assert!(evaluate_scalar("div", &[Value::I64(10), Value::I64(0)]).is_err());
    }

    #[test]
    fn test_width_bucket() {
        assert_eq!(
            evaluate_scalar("width_bucket", &[Value::F64(5.0), Value::F64(0.0), Value::F64(10.0), Value::I64(5)]).unwrap(),
            Value::I64(3)
        );
        // Below min
        assert_eq!(
            evaluate_scalar("width_bucket", &[Value::F64(-1.0), Value::F64(0.0), Value::F64(10.0), Value::I64(5)]).unwrap(),
            Value::I64(0)
        );
        // At or above max
        assert_eq!(
            evaluate_scalar("width_bucket", &[Value::F64(10.0), Value::F64(0.0), Value::F64(10.0), Value::I64(5)]).unwrap(),
            Value::I64(6)
        );
    }

    // -----------------------------------------------------------------------
    // Tests for additional date/time functions
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_minute_second() {
        let ns = 19737_i64 * NANOS_PER_DAY + 10 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN + 45 * NANOS_PER_SEC;
        let ts = Value::Timestamp(ns);
        assert_eq!(evaluate_scalar("extract_minute", &[ts.clone()]).unwrap(), Value::I64(30));
        assert_eq!(evaluate_scalar("extract_second", &[ts]).unwrap(), Value::I64(45));
    }

    #[test]
    fn test_extract_quarter() {
        // Jan 15, 2024
        let ns = 19737_i64 * NANOS_PER_DAY;
        assert_eq!(
            evaluate_scalar("extract_quarter", &[Value::Timestamp(ns)]).unwrap(),
            Value::I64(1)
        );
    }

    #[test]
    fn test_make_timestamp() {
        let result = evaluate_scalar("make_timestamp", &[
            Value::I64(2024), Value::I64(1), Value::I64(15),
            Value::I64(10), Value::I64(30), Value::I64(0),
        ]).unwrap();
        let expected = 19737_i64 * NANOS_PER_DAY + 10 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN;
        assert_eq!(result, Value::Timestamp(expected));
    }

    #[test]
    fn test_interval_to_nanos() {
        assert_eq!(
            evaluate_scalar("interval_to_nanos", &[Value::Str("1h".into())]).unwrap(),
            Value::I64(NANOS_PER_HOUR)
        );
        assert_eq!(
            evaluate_scalar("interval_to_nanos", &[Value::Str("30m".into())]).unwrap(),
            Value::I64(30 * NANOS_PER_MIN)
        );
        assert_eq!(
            evaluate_scalar("interval_to_nanos", &[Value::Str("500ms".into())]).unwrap(),
            Value::I64(500_000_000)
        );
    }

    #[test]
    fn test_days_in_month_and_leap_year() {
        // Feb 2024 (leap year)
        let feb_2024 = civil_to_days(2024, 2, 15) * NANOS_PER_DAY;
        assert_eq!(
            evaluate_scalar("days_in_month_fn", &[Value::Timestamp(feb_2024)]).unwrap(),
            Value::I64(29)
        );
        assert_eq!(
            evaluate_scalar("is_leap_year_fn", &[Value::Timestamp(feb_2024)]).unwrap(),
            Value::I64(1)
        );
        // Feb 2023 (not leap year)
        let feb_2023 = civil_to_days(2023, 2, 15) * NANOS_PER_DAY;
        assert_eq!(
            evaluate_scalar("days_in_month_fn", &[Value::Timestamp(feb_2023)]).unwrap(),
            Value::I64(28)
        );
        assert_eq!(
            evaluate_scalar("is_leap_year_fn", &[Value::Timestamp(feb_2023)]).unwrap(),
            Value::I64(0)
        );
    }

    #[test]
    fn test_first_last_of_month() {
        // Some day in Jan 2024
        let jan15 = civil_to_days(2024, 1, 15) * NANOS_PER_DAY + 10 * NANOS_PER_HOUR;
        assert_eq!(
            evaluate_scalar("first_of_month", &[Value::Timestamp(jan15)]).unwrap(),
            Value::Timestamp(civil_to_days(2024, 1, 1) * NANOS_PER_DAY)
        );
        assert_eq!(
            evaluate_scalar("last_of_month", &[Value::Timestamp(jan15)]).unwrap(),
            Value::Timestamp(civil_to_days(2024, 1, 31) * NANOS_PER_DAY)
        );
    }

    #[test]
    fn test_months_between() {
        let ts1 = civil_to_days(2024, 6, 15) * NANOS_PER_DAY;
        let ts2 = civil_to_days(2024, 1, 15) * NANOS_PER_DAY;
        assert_eq!(
            evaluate_scalar("months_between", &[Value::Timestamp(ts1), Value::Timestamp(ts2)]).unwrap(),
            Value::I64(5)
        );
    }

    #[test]
    fn test_years_between() {
        let ts1 = civil_to_days(2024, 6, 15) * NANOS_PER_DAY;
        let ts2 = civil_to_days(2020, 1, 15) * NANOS_PER_DAY;
        assert_eq!(
            evaluate_scalar("years_between", &[Value::Timestamp(ts1), Value::Timestamp(ts2)]).unwrap(),
            Value::I64(4)
        );
    }

    #[test]
    fn test_ln_alias() {
        // ln should be an alias for log (natural log)
        let result = evaluate_scalar("ln", &[Value::F64(std::f64::consts::E)]).unwrap();
        if let Value::F64(v) = result {
            assert!((v - 1.0).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_to_date() {
        let result = evaluate_scalar("to_date", &[
            Value::Str("2024-03-15".into()),
            Value::Str("yyyy-mm-dd".into()),
        ]).unwrap();
        let expected = civil_to_days(2024, 3, 15) * NANOS_PER_DAY;
        assert_eq!(result, Value::Timestamp(expected));
    }

    #[test]
    fn test_to_number() {
        assert_eq!(evaluate_scalar("to_number", &[Value::Str("42".into())]).unwrap(), Value::I64(42));
        assert_eq!(evaluate_scalar("to_number", &[Value::Str("3.14".into())]).unwrap(), Value::F64(3.14));
    }

    #[test]
    fn test_char_length() {
        assert_eq!(
            evaluate_scalar("char_length", &[Value::Str("hello".into())]).unwrap(),
            Value::I64(5)
        );
    }

    // ── CAST function tests ────────────────────────────────────────

    #[test]
    fn test_cast_to_int() {
        assert_eq!(evaluate_scalar("cast_to_int", &[Value::F64(3.14)]).unwrap(), Value::I64(3));
        assert_eq!(evaluate_scalar("cast_to_int", &[Value::Str("42".into())]).unwrap(), Value::I64(42));
        assert_eq!(evaluate_scalar("cast_to_int", &[Value::I64(100)]).unwrap(), Value::I64(100));
        assert_eq!(evaluate_scalar("cast_to_int", &[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn test_cast_to_float() {
        assert_eq!(evaluate_scalar("cast_to_float", &[Value::I64(42)]).unwrap(), Value::F64(42.0));
        assert_eq!(evaluate_scalar("cast_to_float", &[Value::Str("3.14".into())]).unwrap(), Value::F64(3.14));
        assert_eq!(evaluate_scalar("cast_to_float", &[Value::F64(2.5)]).unwrap(), Value::F64(2.5));
        assert_eq!(evaluate_scalar("cast_to_float", &[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn test_cast_to_str() {
        assert_eq!(evaluate_scalar("cast_to_str", &[Value::I64(42)]).unwrap(), Value::Str("42".into()));
        assert_eq!(evaluate_scalar("cast_to_str", &[Value::F64(3.14)]).unwrap(), Value::Str("3.14".into()));
        assert_eq!(evaluate_scalar("cast_to_str", &[Value::Str("hello".into())]).unwrap(), Value::Str("hello".into()));
    }

    #[test]
    fn test_cast_to_timestamp() {
        // From integer
        assert_eq!(evaluate_scalar("cast_to_timestamp", &[Value::I64(1000000000)]).unwrap(), Value::Timestamp(1000000000));
        // From null
        assert_eq!(evaluate_scalar("cast_to_timestamp", &[Value::Null]).unwrap(), Value::Null);
        // From ISO date string
        let result = evaluate_scalar("cast_to_timestamp", &[Value::Str("2024-01-01".into())]).unwrap();
        match result {
            Value::Timestamp(ns) => assert!(ns > 0, "expected positive timestamp, got {ns}"),
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Tests for rnd_ functions
    // -----------------------------------------------------------------------

    #[test]
    fn test_rnd_int_in_range() {
        for _ in 0..20 {
            let result = evaluate_scalar("rnd_int", &[Value::I64(0), Value::I64(100)]).unwrap();
            match result {
                Value::I64(v) => assert!(v >= 0 && v <= 100, "rnd_int out of range: {v}"),
                other => panic!("expected I64, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_rnd_double_in_range() {
        for _ in 0..20 {
            let result = evaluate_scalar("rnd_double", &[]).unwrap();
            match result {
                Value::F64(v) => assert!(v >= 0.0 && v <= 1.0, "rnd_double out of range: {v}"),
                other => panic!("expected F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_rnd_str_picks_from_args() {
        let args = vec![
            Value::Str("BTC".into()),
            Value::Str("ETH".into()),
            Value::Str("SOL".into()),
        ];
        for _ in 0..20 {
            let result = evaluate_scalar("rnd_str", &args).unwrap();
            match &result {
                Value::Str(s) => assert!(["BTC", "ETH", "SOL"].contains(&s.as_str()), "unexpected: {s}"),
                other => panic!("expected Str, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_rnd_boolean() {
        for _ in 0..20 {
            let result = evaluate_scalar("rnd_boolean", &[]).unwrap();
            match result {
                Value::I64(v) => assert!(v == 0 || v == 1, "rnd_boolean out of range: {v}"),
                other => panic!("expected I64, got {other:?}"),
            }
        }
    }

    // ── Tests for new scalar functions (200+ batch) ────────────────

    #[test]
    fn test_epoch_seconds_millis_micros() {
        let ns = 1_500_000_000_000_000_000_i64; // 1.5 billion seconds
        let ts = Value::Timestamp(ns);
        assert_eq!(evaluate_scalar("epoch_seconds", &[ts.clone()]).unwrap(), Value::I64(1_500_000_000));
        assert_eq!(evaluate_scalar("epoch_millis", &[ts.clone()]).unwrap(), Value::I64(1_500_000_000_000));
        assert_eq!(evaluate_scalar("epoch_micros", &[ts]).unwrap(), Value::I64(1_500_000_000_000_000));
    }

    #[test]
    fn test_to_timezone_and_to_utc() {
        let ns = 19737_i64 * NANOS_PER_DAY; // some day
        let adjusted = evaluate_scalar("to_timezone", &[Value::Timestamp(ns), Value::Str("EST".into())]).unwrap();
        match adjusted {
            Value::Timestamp(adj_ns) => assert_eq!(adj_ns, ns - 5 * NANOS_PER_HOUR),
            _ => panic!("expected Timestamp"),
        }
        let back = evaluate_scalar("to_utc", &[adjusted, Value::Str("EST".into())]).unwrap();
        assert_eq!(back, Value::Timestamp(ns));
    }

    #[test]
    fn test_date_format() {
        let ns = civil_to_days(2024, 3, 15) * NANOS_PER_DAY + 14 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN;
        let result = evaluate_scalar("date_format", &[
            Value::Timestamp(ns),
            Value::Str("%Y-%m-%d %H:%M:%S".into()),
        ]).unwrap();
        assert_eq!(result, Value::Str("2024-03-15 14:30:00".into()));
    }

    #[test]
    fn test_is_weekend_and_business_day() {
        // 2024-03-16 is Saturday
        let sat = civil_to_days(2024, 3, 16) * NANOS_PER_DAY;
        assert_eq!(evaluate_scalar("is_weekend", &[Value::Timestamp(sat)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("is_business_day", &[Value::Timestamp(sat)]).unwrap(), Value::I64(0));
        // 2024-03-15 is Friday
        let fri = civil_to_days(2024, 3, 15) * NANOS_PER_DAY;
        assert_eq!(evaluate_scalar("is_weekend", &[Value::Timestamp(fri)]).unwrap(), Value::I64(0));
        assert_eq!(evaluate_scalar("is_business_day", &[Value::Timestamp(fri)]).unwrap(), Value::I64(1));
    }

    #[test]
    fn test_char_at() {
        assert_eq!(evaluate_scalar("char_at", &[Value::Str("hello".into()), Value::I64(1)]).unwrap(), Value::Str("h".into()));
        assert_eq!(evaluate_scalar("char_at", &[Value::Str("hello".into()), Value::I64(5)]).unwrap(), Value::Str("o".into()));
        assert_eq!(evaluate_scalar("char_at", &[Value::Str("hello".into()), Value::I64(10)]).unwrap(), Value::Null);
    }

    #[test]
    fn test_hex_unhex() {
        assert_eq!(evaluate_scalar("hex", &[Value::I64(255)]).unwrap(), Value::Str("ff".into()));
        assert_eq!(evaluate_scalar("unhex", &[Value::Str("ff".into())]).unwrap(), Value::I64(255));
        assert_eq!(evaluate_scalar("unhex", &[Value::Str("1a".into())]).unwrap(), Value::I64(26));
    }

    #[test]
    fn test_url_encode_decode() {
        let encoded = evaluate_scalar("url_encode", &[Value::Str("hello world!".into())]).unwrap();
        assert_eq!(encoded, Value::Str("hello%20world%21".into()));
        let decoded = evaluate_scalar("url_decode", &[encoded]).unwrap();
        assert_eq!(decoded, Value::Str("hello world!".into()));
    }

    #[test]
    fn test_json_extract() {
        let json = r#"{"name":"Alice","age":30,"nested":{"x":42}}"#;
        assert_eq!(
            evaluate_scalar("json_extract", &[Value::Str(json.into()), Value::Str("name".into())]).unwrap(),
            Value::Str("Alice".into())
        );
        assert_eq!(
            evaluate_scalar("json_extract", &[Value::Str(json.into()), Value::Str("age".into())]).unwrap(),
            Value::I64(30)
        );
        assert_eq!(
            evaluate_scalar("json_extract", &[Value::Str(json.into()), Value::Str("nested.x".into())]).unwrap(),
            Value::I64(42)
        );
    }

    #[test]
    fn test_json_array_length() {
        assert_eq!(
            evaluate_scalar("json_array_length", &[Value::Str("[1,2,3]".into())]).unwrap(),
            Value::I64(3)
        );
        assert_eq!(
            evaluate_scalar("json_array_length", &[Value::Str("[]".into())]).unwrap(),
            Value::I64(0)
        );
    }

    #[test]
    fn test_clamp() {
        assert_eq!(evaluate_scalar("clamp", &[Value::F64(5.0), Value::F64(0.0), Value::F64(10.0)]).unwrap(), Value::F64(5.0));
        assert_eq!(evaluate_scalar("clamp", &[Value::F64(-1.0), Value::F64(0.0), Value::F64(10.0)]).unwrap(), Value::F64(0.0));
        assert_eq!(evaluate_scalar("clamp", &[Value::F64(15.0), Value::F64(0.0), Value::F64(10.0)]).unwrap(), Value::F64(10.0));
    }

    #[test]
    fn test_lerp() {
        assert_eq!(evaluate_scalar("lerp", &[Value::F64(0.0), Value::F64(10.0), Value::F64(0.5)]).unwrap(), Value::F64(5.0));
        assert_eq!(evaluate_scalar("lerp", &[Value::F64(0.0), Value::F64(10.0), Value::F64(0.0)]).unwrap(), Value::F64(0.0));
        assert_eq!(evaluate_scalar("lerp", &[Value::F64(0.0), Value::F64(10.0), Value::F64(1.0)]).unwrap(), Value::F64(10.0));
    }

    #[test]
    fn test_map_range() {
        let result = evaluate_scalar("map_range", &[
            Value::F64(5.0), Value::F64(0.0), Value::F64(10.0), Value::F64(0.0), Value::F64(100.0)
        ]).unwrap();
        assert_eq!(result, Value::F64(50.0));
    }

    #[test]
    fn test_is_finite_nan_inf() {
        assert_eq!(evaluate_scalar("is_finite", &[Value::F64(1.0)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("is_finite", &[Value::F64(f64::INFINITY)]).unwrap(), Value::I64(0));
        assert_eq!(evaluate_scalar("is_nan", &[Value::F64(f64::NAN)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("is_nan", &[Value::F64(1.0)]).unwrap(), Value::I64(0));
        assert_eq!(evaluate_scalar("is_inf", &[Value::F64(f64::INFINITY)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("is_inf", &[Value::F64(1.0)]).unwrap(), Value::I64(0));
    }

    #[test]
    fn test_fma() {
        assert_eq!(evaluate_scalar("fma", &[Value::F64(2.0), Value::F64(3.0), Value::F64(4.0)]).unwrap(), Value::F64(10.0));
    }

    #[test]
    fn test_hypot() {
        assert_eq!(evaluate_scalar("hypot", &[Value::F64(3.0), Value::F64(4.0)]).unwrap(), Value::F64(5.0));
    }

    #[test]
    fn test_copysign() {
        assert_eq!(evaluate_scalar("copysign", &[Value::F64(5.0), Value::F64(-1.0)]).unwrap(), Value::F64(-5.0));
        assert_eq!(evaluate_scalar("copysign", &[Value::F64(-5.0), Value::F64(1.0)]).unwrap(), Value::F64(5.0));
    }

    #[test]
    fn test_next_power_of_two() {
        assert_eq!(evaluate_scalar("next_power_of_two", &[Value::I64(5)]).unwrap(), Value::I64(8));
        assert_eq!(evaluate_scalar("next_power_of_two", &[Value::I64(8)]).unwrap(), Value::I64(8));
        assert_eq!(evaluate_scalar("next_power_of_two", &[Value::I64(1)]).unwrap(), Value::I64(1));
    }

    #[test]
    fn test_version() {
        let result = evaluate_scalar("version", &[]).unwrap();
        match result {
            Value::Str(s) => assert!(s.contains("ExchangeDB")),
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn test_sizeof() {
        assert_eq!(evaluate_scalar("sizeof", &[Value::I64(42)]).unwrap(), Value::I64(8));
        assert_eq!(evaluate_scalar("sizeof", &[Value::Str("hello".into())]).unwrap(), Value::I64(5));
        assert_eq!(evaluate_scalar("sizeof", &[Value::Null]).unwrap(), Value::I64(0));
    }

    #[test]
    fn test_pg_typeof() {
        assert_eq!(evaluate_scalar("pg_typeof", &[Value::I64(1)]).unwrap(), Value::Str("bigint".into()));
        assert_eq!(evaluate_scalar("pg_typeof", &[Value::F64(1.0)]).unwrap(), Value::Str("double precision".into()));
        assert_eq!(evaluate_scalar("pg_typeof", &[Value::Str("hi".into())]).unwrap(), Value::Str("text".into()));
    }

    #[test]
    fn test_crc32() {
        let result = evaluate_scalar("crc32", &[Value::Str("hello".into())]).unwrap();
        // CRC32 of "hello" = 0x3610A686 = 907060870
        assert_eq!(result, Value::I64(907060870));
    }

    #[test]
    fn test_to_json() {
        assert_eq!(evaluate_scalar("to_json", &[Value::I64(42)]).unwrap(), Value::Str("42".into()));
        assert_eq!(evaluate_scalar("to_json", &[Value::Str("hello".into())]).unwrap(), Value::Str("\"hello\"".into()));
        let multi = evaluate_scalar("to_json", &[Value::I64(1), Value::I64(2), Value::I64(3)]).unwrap();
        assert_eq!(multi, Value::Str("[1,2,3]".into()));
    }

    #[test]
    fn test_iif() {
        assert_eq!(evaluate_scalar("iif", &[Value::I64(1), Value::Str("yes".into()), Value::Str("no".into())]).unwrap(), Value::Str("yes".into()));
        assert_eq!(evaluate_scalar("iif", &[Value::I64(0), Value::Str("yes".into()), Value::Str("no".into())]).unwrap(), Value::Str("no".into()));
    }

    #[test]
    fn test_nvl2() {
        assert_eq!(evaluate_scalar("nvl2", &[Value::I64(1), Value::Str("not null".into()), Value::Str("null".into())]).unwrap(), Value::Str("not null".into()));
        assert_eq!(evaluate_scalar("nvl2", &[Value::Null, Value::Str("not null".into()), Value::Str("null".into())]).unwrap(), Value::Str("null".into()));
    }

    #[test]
    fn test_soundex() {
        assert_eq!(evaluate_scalar("soundex", &[Value::Str("Robert".into())]).unwrap(), Value::Str("R163".into()));
        assert_eq!(evaluate_scalar("soundex", &[Value::Str("Smith".into())]).unwrap(), Value::Str("S530".into()));
    }

    #[test]
    fn test_regexp_count() {
        assert_eq!(evaluate_scalar("regexp_count", &[Value::Str("aababab".into()), Value::Str("ab".into())]).unwrap(), Value::I64(3));
    }

    #[test]
    fn test_word_count() {
        assert_eq!(evaluate_scalar("word_count", &[Value::Str("hello world foo".into())]).unwrap(), Value::I64(3));
        assert_eq!(evaluate_scalar("word_count", &[Value::Str("".into())]).unwrap(), Value::I64(0));
    }

    #[test]
    fn test_camel_snake_case() {
        assert_eq!(evaluate_scalar("camel_case", &[Value::Str("hello_world".into())]).unwrap(), Value::Str("HelloWorld".into()));
        assert_eq!(evaluate_scalar("snake_case", &[Value::Str("HelloWorld".into())]).unwrap(), Value::Str("hello_world".into()));
    }

    #[test]
    fn test_numeric_predicates() {
        assert_eq!(evaluate_scalar("is_positive", &[Value::I64(5)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("is_negative", &[Value::I64(-5)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("is_zero", &[Value::I64(0)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("is_even", &[Value::I64(4)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("is_odd", &[Value::I64(3)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("between", &[Value::F64(5.0), Value::F64(1.0), Value::F64(10.0)]).unwrap(), Value::I64(1));
        assert_eq!(evaluate_scalar("between", &[Value::F64(15.0), Value::F64(1.0), Value::F64(10.0)]).unwrap(), Value::I64(0));
    }

    #[test]
    fn test_start_of_year_and_end_of_year() {
        let ns = civil_to_days(2024, 6, 15) * NANOS_PER_DAY;
        assert_eq!(evaluate_scalar("start_of_year", &[Value::Timestamp(ns)]).unwrap(), Value::Timestamp(civil_to_days(2024, 1, 1) * NANOS_PER_DAY));
        assert_eq!(evaluate_scalar("end_of_year", &[Value::Timestamp(ns)]).unwrap(), Value::Timestamp(civil_to_days(2024, 12, 31) * NANOS_PER_DAY));
    }

    #[test]
    fn test_squeeze() {
        assert_eq!(evaluate_scalar("squeeze", &[Value::Str("  hello   world  ".into())]).unwrap(), Value::Str("hello world".into()));
    }

    #[test]
    fn test_zeroifnull_nullifempty() {
        assert_eq!(evaluate_scalar("zeroifnull", &[Value::Null]).unwrap(), Value::I64(0));
        assert_eq!(evaluate_scalar("zeroifnull", &[Value::I64(5)]).unwrap(), Value::I64(5));
        assert_eq!(evaluate_scalar("nullifempty", &[Value::Str("".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate_scalar("nullifempty", &[Value::Str("hi".into())]).unwrap(), Value::Str("hi".into()));
    }

    #[test]
    fn test_e_tau_constants() {
        assert_eq!(evaluate_scalar("e", &[]).unwrap(), Value::F64(std::f64::consts::E));
        assert_eq!(evaluate_scalar("tau", &[]).unwrap(), Value::F64(std::f64::consts::TAU));
    }

    #[test]
    fn test_aliases_work() {
        // Test that aliases resolve correctly
        assert_eq!(evaluate_scalar("to_lowercase", &[Value::Str("HELLO".into())]).unwrap(), Value::Str("hello".into()));
        assert_eq!(evaluate_scalar("to_uppercase", &[Value::Str("hello".into())]).unwrap(), Value::Str("HELLO".into()));
        assert_eq!(evaluate_scalar("systimestamp", &[]).is_ok(), true);
        assert_eq!(evaluate_scalar("sysdate", &[]).is_ok(), true);
        assert_eq!(evaluate_scalar("ceiling", &[Value::F64(3.2)]).unwrap(), Value::F64(4.0));
        assert_eq!(evaluate_scalar("power", &[Value::F64(2.0), Value::F64(3.0)]).unwrap(), Value::F64(8.0));
        assert_eq!(evaluate_scalar("len", &[Value::Str("hi".into())]).unwrap(), Value::I64(2));
    }

    #[test]
    fn test_base64_round_trip() {
        let encoded = evaluate_scalar("to_base64", &[Value::Str("Hello, World!".into())]).unwrap();
        assert_eq!(encoded, Value::Str("SGVsbG8sIFdvcmxkIQ==".into()));
        let decoded = evaluate_scalar("from_base64", &[encoded]).unwrap();
        assert_eq!(decoded, Value::Str("Hello, World!".into()));
    }

    #[test]
    fn test_rnd_uuid4_format() {
        let result = evaluate_scalar("rnd_uuid4", &[]).unwrap();
        match &result {
            Value::Str(s) => {
                assert_eq!(s.len(), 36, "UUID wrong length: {s}");
                assert_eq!(s.chars().filter(|&c| c == '-').count(), 4, "UUID wrong format: {s}");
            }
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn test_rnd_timestamp_in_range() {
        let lo = civil_to_days(2024, 1, 1) * NANOS_PER_DAY;
        let hi = civil_to_days(2024, 12, 31) * NANOS_PER_DAY;
        for _ in 0..20 {
            let result = evaluate_scalar("rnd_timestamp", &[Value::I64(lo), Value::I64(hi)]).unwrap();
            match result {
                Value::Timestamp(ns) => assert!(ns >= lo && ns <= hi, "rnd_timestamp out of range"),
                other => panic!("expected Timestamp, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_total_scalar_function_count() {
        let registry = ScalarRegistry::new();
        let count = registry.len();
        eprintln!("Total scalar functions registered: {count}");
        assert!(count >= 1046, "Expected 1046+ scalar functions, got {count}");
    }
}
