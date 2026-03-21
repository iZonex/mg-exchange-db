/// Nanosecond-precision timestamp (nanos since Unix epoch).
/// Supports dates from 1677-09-21 to 2262-04-11.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub const NULL: Self = Self(i64::MIN);
    pub const MIN: Self = Self(i64::MIN + 1);
    pub const MAX: Self = Self(i64::MAX);

    #[inline]
    pub fn now() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos() as i64;
        Self(nanos)
    }

    #[inline]
    pub fn from_micros(us: i64) -> Self {
        Self(us * 1_000)
    }

    #[inline]
    pub fn from_millis(ms: i64) -> Self {
        Self(ms * 1_000_000)
    }

    #[inline]
    pub fn from_secs(s: i64) -> Self {
        Self(s * 1_000_000_000)
    }

    #[inline]
    pub fn as_nanos(self) -> i64 {
        self.0
    }

    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == i64::MIN
    }
}

/// Column data types supported by the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ColumnType {
    Boolean = 0,
    I8 = 1,
    I16 = 2,
    I32 = 3,
    I64 = 4,
    F32 = 5,
    F64 = 6,
    Timestamp = 7,
    Symbol = 8,
    Varchar = 9,
    Binary = 10,
    Uuid = 11,
    Date = 12,      // i32, days since epoch (like QuestDB)
    Char = 13,      // u16, single UTF-16 character
    IPv4 = 14,      // u32, IPv4 address
    Long128 = 15,   // i128, 128-bit integer
    Long256 = 16,   // [u64; 4], 256-bit integer
    GeoHash = 17,   // i64, geospatial hash

    // --- New types (18–39) to match QuestDB's full 41-type catalog ---

    // String variant
    String = 18,         // Same as Varchar but explicit (QuestDB distinction)

    // Temporal variants
    TimestampMicro = 19, // Microsecond precision timestamp
    TimestampMilli = 20, // Millisecond precision timestamp
    Interval = 21,       // Time interval (two i64: start, duration)

    // Decimal variants
    Decimal8 = 22,       // 1-byte decimal
    Decimal16 = 23,      // 2-byte decimal
    Decimal32 = 24,      // 4-byte decimal
    Decimal64 = 25,      // 8-byte decimal
    Decimal128 = 26,     // 16-byte decimal
    Decimal256 = 27,     // 32-byte decimal

    // Geospatial variants (sized geohash)
    GeoByte = 28,        // 1-byte geohash (1-7 bits)
    GeoShort = 29,       // 2-byte geohash (8-15 bits)
    GeoInt = 30,         // 4-byte geohash (16-31 bits)

    // Container types
    Array = 31,          // Array type (variable-length, length-prefixed binary)
    Cursor = 32,         // Cursor reference (internal)
    Record = 33,         // Record reference (internal)

    // PostgreSQL compatibility
    RegClass = 34,       // pg regclass
    RegProcedure = 35,   // pg regprocedure
    ArrayString = 36,    // pg text[]

    // Special / internal
    Null = 37,           // Explicit NULL type
    VarArg = 38,         // Variable arguments (internal)
    Parameter = 39,      // Bind parameter (internal)
    VarcharSlice = 40,   // Transient in-memory slice into varchar data (internal use)
    IPv6 = 41,           // [u8; 16], IPv6 address
}

impl ColumnType {
    /// Size in bytes for fixed-width types. Returns None for variable-width types.
    pub fn fixed_size(self) -> Option<usize> {
        match self {
            Self::Boolean | Self::I8 => Some(1),
            Self::I16 | Self::Char => Some(2),
            Self::I32 | Self::F32 | Self::Symbol | Self::Date | Self::IPv4 => Some(4),
            Self::I64 | Self::F64 | Self::Timestamp | Self::GeoHash
            | Self::TimestampMicro | Self::TimestampMilli | Self::Decimal64
            | Self::Cursor | Self::Record => Some(8),
            Self::Uuid | Self::Long128 | Self::Decimal128 | Self::Interval => Some(16),
            Self::Long256 | Self::Decimal256 => Some(32),
            Self::Decimal8 | Self::GeoByte => Some(1),
            Self::Decimal16 | Self::GeoShort => Some(2),
            Self::Decimal32 | Self::GeoInt | Self::RegClass | Self::RegProcedure => Some(4),
            Self::Null | Self::VarArg | Self::Parameter => Some(0),
            Self::IPv6 => Some(16),
            Self::Varchar | Self::Binary | Self::String | Self::Array | Self::ArrayString
            | Self::VarcharSlice => None,
        }
    }

    /// True if this type requires a secondary index file for offsets.
    pub fn is_variable_length(self) -> bool {
        self.fixed_size().is_none()
    }
}

/// Partition granularity for time-based partitioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartitionBy {
    None,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl PartitionBy {
    /// Format string for partition directory names.
    pub fn dir_format(self) -> &'static str {
        match self {
            Self::None => "default",
            Self::Hour => "%Y-%m-%dT%H",
            Self::Day => "%Y-%m-%d",
            Self::Week => "%Y-W%W",
            Self::Month => "%Y-%m",
            Self::Year => "%Y",
        }
    }
}
